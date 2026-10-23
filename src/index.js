// ============================================================
// HUNTER LEAD WORKER
//
// Trigger: Supabase Cron sends POST
//
// Workflow:
// 1.  Find oldest pending outreach task.
// 2.  Claim the task.
// 3.  Parse the task text into structured filters.
// 4.  Discover matching companies using Hunter (free).
// 5.  Pick one company not already used for this application.
// 6.  Perform at most ONE paid Domain Search.
// 7.  Select one suitable email and avoid duplicates.
// 8.  Save one lead to Supabase.
// 9.  Decrease pending_count after a new lead is saved.
// 10. Mark task Completed when pending_count reaches zero.
//
// Required Cloudflare secrets:
//   SUPABASE_URL
//   SUPABASE_SERVICE_ROLE_KEY
//   COMPOSIO_API_KEY
//
// Optional variables:
//   COMPOSIO_HUNTER_VERSION        Composio tool version
//   HUNTER_USE_DOMAIN_FILTERS      "false" to skip seniority/department
//                                  filters on Domain Search
// ============================================================

const COMPOSIO_BASE_URL = "https://backend.composio.dev/api/v3.1";
const DEFAULT_HUNTER_VERSION = "20261018_00";
const DISCOVER_LIMIT = 25;
const DOMAIN_SEARCH_LIMIT = 10;

// ============================================================
// HTTP ENTRY POINT
// ============================================================

export default {
  async fetch(request, env) {
    if (request.method !== "POST") {
      return json({ ok: false, error: "POST requests only" }, 405);
    }

    try {
      validateEnvironment(env);
      const result = await processOneTask(env);
      return json(result);
    } catch (error) {
      console.error("Worker failed:", error.message);
      return json({ ok: false, error: error.message }, 500);
    }
  },
};

// ============================================================
// ENVIRONMENT
// ============================================================

function validateEnvironment(env) {
  const required = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "COMPOSIO_API_KEY",
  ];

  for (const name of required) {
    if (!env[name]) {
      throw new Error(`Missing environment variable: ${name}`);
    }
  }
}

function getHunterVersion(env) {
  return env.COMPOSIO_HUNTER_VERSION || DEFAULT_HUNTER_VERSION;
}

// ============================================================
// SUPABASE REST HELPER
// ============================================================

async function supabaseRequest(env, path, options = {}) {
  const base = env.SUPABASE_URL.replace(/\/+$/, "");

  const response = await fetch(`${base}/rest/v1/${path}`, {
    ...options,
    headers: {
      apikey: env.SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
      ...options.headers,
    },
  });

  const raw = await response.text();
  let data = null;

  if (raw) {
    try {
      data = JSON.parse(raw);
    } catch {
      data = raw;
    }
  }

  if (!response.ok) {
    console.error(
      "Supabase request failed:",
      response.status,
      JSON.stringify(data).slice(0, 1200)
    );
    throw new Error(`Supabase request failed: HTTP ${response.status}`);
  }

  return { data, headers: response.headers };
}

// ============================================================
// PROCESS ONE TASK
// ============================================================

async function processOneTask(env) {
  const task = await findOldestPendingTask(env);

  if (!task) {
    return {
      ok: true,
      processed: false,
      message: "No pending outreach tasks",
    };
  }

  const claimed = await claimTask(env, task);

  if (!claimed) {
    return {
      ok: true,
      processed: false,
      message: "Task was already claimed",
      task_id: task.id,
    };
  }

  try {
    const result = await findAndSaveOneLead(env, task);

    if (!result.saved) {
      await releaseTask(env, task.id, result.reason);

      return {
        ok: true,
        processed: false,
        task_id: task.id,
        reason: result.reason,
        pending_count_unchanged: true,
      };
    }

    const updatedTask = await decrementPendingCount(env, task);

    return {
      ok: true,
      processed: true,
      task_id: task.id,
      lead: result.lead,
      pending_count: updatedTask.pending_count,
      status: updatedTask.status,
    };
  } catch (error) {
    await releaseTask(env, task.id, error.message);
    throw error;
  }
}

// ============================================================
// FIND OLDEST PENDING OUTREACH TASK
// ============================================================

async function findOldestPendingTask(env) {
  const query =
    "planner_runs" +
    "?select=*" +
    "&source=eq.outreach" +
    "&worker_type=eq.cold_outreach" +
    "&status=eq.pending" +
    "&pending_count=gt.0" +
    "&order=created_at.asc" +
    "&limit=1";

  const result = await supabaseRequest(env, query);

  if (!Array.isArray(result.data) || result.data.length === 0) {
    return null;
  }

  return result.data[0];
}

// ============================================================
// CLAIM TASK
// ============================================================

async function claimTask(env, task) {
  const query =
    "planner_runs" +
    `?id=eq.${encodeURIComponent(task.id)}` +
    "&status=eq.pending";

  const result = await supabaseRequest(env, query, {
    method: "PATCH",
    headers: { Prefer: "return=representation" },
    body: JSON.stringify({
      status: "Processing",
      started_at: new Date().toISOString(),
      error: null,
    }),
  });

  return Array.isArray(result.data) && result.data.length === 1;
}

// ============================================================
// RELEASE TASK FOR RETRY
// ============================================================

async function releaseTask(env, taskId, reason) {
  const query =
    "planner_runs" +
    `?id=eq.${encodeURIComponent(taskId)}` +
    "&status=eq.Processing";

  await supabaseRequest(env, query, {
    method: "PATCH",
    body: JSON.stringify({
      status: "pending",
      error: String(reason || "Unknown error").slice(0, 2000),
    }),
  });
}

// ============================================================
// TASK PARSING
//
// Turns the free-text task into structured filters.
// ============================================================

const HEADCOUNT_BUCKETS = [
  [1, 10, "1-10"],
  [11, 50, "11-50"],
  [51, 200, "51-200"],
  [201, 500, "201-500"],
  [501, 1000, "501-1000"],
  [1001, 5000, "1001-5000"],
  [5001, 10000, "5001-10000"],
  [10001, Infinity, "10001+"],
];

const COUNTRY_CODES = {
  "united states": "US",
  "united states of america": "US",
  usa: "US",
  us: "US",
  "united kingdom": "GB",
  uk: "GB",
  canada: "CA",
  india: "IN",
  australia: "AU",
  germany: "DE",
  france: "FR",
  spain: "ES",
  italy: "IT",
  netherlands: "NL",
  ireland: "IE",
  singapore: "SG",
  "united arab emirates": "AE",
  uae: "AE",
};

function parseTask(task) {
  const text = String(task.task || "");
  const lines = text.split(/\r?\n/).map((line) => line.trim());

  function getLine(label) {
    const re = new RegExp(`^-?\\s*${label}\\s*:\\s*(.+)$`, "i");

    for (const line of lines) {
      const match = line.match(re);
      if (match) {
        return match[1].replace(/\.\s*$/, "").trim();
      }
    }

    return null;
  }

  // Niche
  const niche = String(
    task.niche || getLine("Customer Niche") || "B2B companies"
  ).slice(0, 300);

  // Recommended job titles
  const jobTitles = [];
  const titlesIndex = lines.findIndex((line) =>
    /^recommended job titles/i.test(line)
  );

  if (titlesIndex !== -1) {
    for (let i = titlesIndex + 1; i < lines.length; i++) {
      if (!lines[i].startsWith("-")) break;
      const title = lines[i].replace(/^-+\s*/, "").trim();
      if (title) jobTitles.push(title);
    }
  }

  // Company keywords
  const keywordLine = getLine("Company keywords");
  const keywords = keywordLine
    ? keywordLine
        .split(/[,;]/)
        .map((k) => k.trim())
        .filter(Boolean)
    : [];

  // Company size
  const sizeLine = getLine("Company size");
  let sizeMin = null;
  let sizeMax = null;

  if (sizeLine) {
    const range = sizeLine.match(/(\d[\d,]*)\s*[–—-]\s*(\d[\d,]*)/);
    const plus = sizeLine.match(/(\d[\d,]*)\s*\+/);

    if (range) {
      sizeMin = Number(range[1].replace(/,/g, ""));
      sizeMax = Number(range[2].replace(/,/g, ""));
    } else if (plus) {
      sizeMin = Number(plus[1].replace(/,/g, ""));
      sizeMax = Infinity;
    }
  }

  const headcount =
    sizeMin !== null
      ? HEADCOUNT_BUCKETS.filter(
          ([low, high]) => low <= sizeMax && high >= sizeMin
        ).map(([, , label]) => label)
      : [];

  // Company location
  const locationLine = getLine("Company location");
  const countryName = locationLine
    ? locationLine.toLowerCase().replace(/[.]/g, "").trim()
    : null;
  const countryCode = countryName ? COUNTRY_CODES[countryName] || null : null;

  // Department + seniority for Domain Search
  const departmentLine = (getLine("Department") || "").toLowerCase();
  const seniorityLine = (getLine("Seniority") || "").toLowerCase();

  const departments = new Set();
  if (/growth|marketing/.test(departmentLine)) departments.add("marketing");
  if (/sales|business development/.test(departmentLine)) {
    departments.add("sales");
  }
  if (/operations/.test(departmentLine)) departments.add("operations");
  if (/executive|management|founder/.test(departmentLine)) {
    departments.add("executive");
    departments.add("management");
  }

  const seniority = new Set();
  if (/founder|c-level|chief|vp|vice president/.test(seniorityLine)) {
    seniority.add("executive");
  }
  if (/head|director|senior/.test(seniorityLine)) {
    seniority.add("senior");
  }

  return {
    niche,
    jobTitles,
    keywords,
    sizeMin,
    sizeMax,
    headcount,
    locationName: locationLine,
    countryCode,
    departments: [...departments],
    seniority: [...seniority],
  };
}

// ============================================================
// FIND AND SAVE ONE LEAD
// ============================================================

async function findAndSaveOneLead(env, task) {
  const filters = parseTask(task);

  // Free discovery step.
  const companies = await discoverCompanies(env, filters);

  if (!companies.length) {
    return {
      saved: false,
      reason: "Hunter discovery returned no usable companies",
    };
  }

  const company = await pickCompany(env, task, companies);

  if (!company) {
    return {
      saved: false,
      reason: "All discovered companies were already used or had no emails",
    };
  }

  // Exactly one paid Domain Search call at most per worker run.
  const domainResult = await searchDomain(env, company.domain, filters);

  const candidates = extractEmailCandidates(domainResult)
    .filter((candidate) => isValidEmail(candidate.email))
    .filter((candidate) => !isBadStatus(candidate.email_status))
    .sort(
      (a, b) => scoreCandidate(b, filters) - scoreCandidate(a, filters)
    );

  if (!candidates.length) {
    console.error(
      `Domain Search found no usable emails for ${company.domain}:`,
      JSON.stringify(domainResult).slice(0, 1500)
    );

    return {
      saved: false,
      reason: `Domain Search returned no usable email addresses for ${company.domain}`,
    };
  }

  for (const candidate of candidates) {
    const email = candidate.email.trim().toLowerCase();

    if (await leadEmailExists(env, task.application_id, email)) {
      continue;
    }

    const lead = buildLead(task, company, { ...candidate, email });
    const inserted = await insertLead(env, lead);

    if (inserted) {
      return {
        saved: true,
        lead: {
          email: lead.email,
          full_name: lead.full_name,
          job_title: lead.job_title,
          company_name: lead.company_name,
          company_domain: lead.company_domain,
        },
      };
    }
  }

  return {
    saved: false,
    reason: "All returned email addresses were duplicates",
  };
}

// ============================================================
// PICK A COMPANY
//
// Skips companies Hunter says have zero emails, skips domains
// already saved for this application, then shuffles so a dead
// domain is not retried on every run.
// ============================================================

async function pickCompany(env, task, companies) {
  let pool = companies.filter((company) => company.emailsTotal !== 0);

  if (!pool.length) return null;

  const usedDomains = await getUsedDomains(
    env,
    task.application_id,
    pool.map((company) => company.domain)
  );

  pool = pool.filter((company) => !usedDomains.has(company.domain));

  if (!pool.length) return null;

  return shuffle(pool)[0];
}

async function getUsedDomains(env, applicationId, domains) {
  const used = new Set();

  if (!domains.length) return used;

  const list = domains.map((d) => encodeURIComponent(d)).join(",");

  const query =
    "leads" +
    "?select=company_domain" +
    `&application_id=eq.${encodeURIComponent(applicationId)}` +
    `&company_domain=in.(${list})`;

  const result = await supabaseRequest(env, query);

  if (Array.isArray(result.data)) {
    for (const row of result.data) {
      if (row.company_domain) {
        used.add(String(row.company_domain).toLowerCase());
      }
    }
  }

  return used;
}

function shuffle(items) {
  const copy = [...items];

  for (let i = copy.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }

  return copy;
}

// ============================================================
// HUNTER COMPANY DISCOVERY (free)
//
// Attempt 1: natural-language query + structured filters.
// Attempt 2: natural-language query only (fallback if the
//            structured arguments are rejected or return nothing).
//
// Check your Composio HUNTER_DISCOVER_COMPANIES schema and
// adjust the argument names in buildDiscoverArguments() if needed.
// ============================================================

function buildDiscoverQuery(filters) {
  const parts = [];

  const topic = filters.keywords.length
    ? filters.keywords.slice(0, 6).join(", ")
    : filters.niche;

  parts.push(`${topic} companies`);

  if (filters.locationName) {
    parts.push(`in ${filters.locationName}`);
  }

  if (filters.sizeMin !== null) {
    const range =
      filters.sizeMax === Infinity
        ? `${filters.sizeMin}+`
        : `${filters.sizeMin}-${filters.sizeMax}`;
    parts.push(`with ${range} employees`);
  }

  return parts.join(" ");
}

function buildDiscoverArguments(filters, query) {
  const args = { query, limit: DISCOVER_LIMIT };

  if (filters.countryCode) {
    args.headquarters_location = {
      include: [{ country: filters.countryCode }],
    };
  }

  if (filters.headcount.length) {
    args.headcount = filters.headcount;
  }

  if (filters.keywords.length) {
    args.keywords = {
      include: filters.keywords.slice(0, 8),
      match: "any",
    };
  }

  return args;
}

async function discoverCompanies(env, filters) {
  const query = buildDiscoverQuery(filters);

  // Attempt 1: structured
  try {
    const response = await executeHunterTool(
      env,
      "HUNTER_DISCOVER_COMPANIES",
      buildDiscoverArguments(filters, query)
    );

    const companies = extractCompanies(response);

    if (companies.length) return companies;

    console.error(
      "Discover (structured) returned no companies. Raw:",
      JSON.stringify(response).slice(0, 1500)
    );
  } catch (error) {
    console.error("Discover (structured) failed:", error.message);
  }

  // Attempt 2: query only
  const response = await executeHunterTool(
    env,
    "HUNTER_DISCOVER_COMPANIES",
    { query }
  );

  const companies = extractCompanies(response);

  if (!companies.length) {
    console.error(
      "Discover (query only) returned no companies. Raw:",
      JSON.stringify(response).slice(0, 1500)
    );
  }

  return companies;
}

// ============================================================
// HUNTER DOMAIN SEARCH (paid)
// ============================================================

async function searchDomain(env, domain, filters) {
  const args = {
    domain,
    type: "personal",
    limit: DOMAIN_SEARCH_LIMIT,
  };

  const useFilters = String(env.HUNTER_USE_DOMAIN_FILTERS || "true")
    .toLowerCase() !== "false";

  if (useFilters) {
    if (filters.seniority.length) {
      args.seniority = filters.seniority.join(",");
    }

    if (filters.departments.length) {
      args.department = filters.departments.join(",");
    }
  }

  return executeHunterTool(env, "HUNTER_DOMAIN_SEARCH", args);
}

// ============================================================
// COMPOSIO EXECUTION (structured arguments)
// ============================================================

async function executeHunterTool(env, toolSlug, args) {
  const response = await fetch(
    `${COMPOSIO_BASE_URL}/tools/execute/${encodeURIComponent(toolSlug)}`,
    {
      method: "POST",
      headers: {
        "x-api-key": env.COMPOSIO_API_KEY,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        connected_account_id: "instant_account",
        version: getHunterVersion(env),
        arguments: args,
      }),
    }
  );

  const raw = await response.text();
  let data;

  try {
    data = raw ? JSON.parse(raw) : {};
  } catch {
    throw new Error(
      `Composio returned non-JSON output for ${toolSlug}: ` +
        raw.slice(0, 500)
    );
  }

  if (!response.ok) {
    throw new Error(
      `Composio ${toolSlug} failed with HTTP ${response.status}: ` +
        JSON.stringify(data).slice(0, 1200)
    );
  }

  if (data.error) {
    throw new Error(
      `Composio ${toolSlug} error: ` + JSON.stringify(data.error).slice(0, 1200)
    );
  }

  if (data.successful === false) {
    throw new Error(
      `Composio ${toolSlug} unsuccessful: ` + JSON.stringify(data).slice(0, 1200)
    );
  }

  return data;
}

// ============================================================
// EXTRACT COMPANIES
//
// Hunter Discover returns items like:
//   { domain, organization, emails_count: { personal, generic, total } }
// ============================================================

function extractCompanies(response) {
  const companies = [];
  const seen = new Set();

  function walk(value, depth = 0) {
    if (!value || depth > 10) return;

    if (Array.isArray(value)) {
      for (const item of value) walk(item, depth + 1);
      return;
    }

    if (typeof value !== "object") return;

    const domain = getCompanyDomain(value);

    if (domain && !seen.has(domain)) {
      seen.add(domain);

      const nameCandidate =
        value.organization ||
        value.company_name ||
        value.company ||
        value.organization_name ||
        value.name;

      const name =
        typeof nameCandidate === "string" && nameCandidate.trim()
          ? nameCandidate.trim()
          : domain;

      companies.push({
        name,
        domain,
        website: value.website || value.company_website || null,
        industry: value.industry || value.company_industry || null,
        size:
          value.employee_count ??
          value.company_size ??
          value.employees ??
          value.headcount ??
          null,
        country: value.country || value.company_country || null,
        city: value.city || value.company_city || null,
        emailsTotal: readEmailsTotal(value.emails_count),
      });
    }

    for (const child of Object.values(value)) {
      if (child && typeof child === "object") {
        walk(child, depth + 1);
      }
    }
  }

  walk(response);
  return companies;
}

// Returns a number when Hunter reports it, otherwise null (unknown).
function readEmailsTotal(emailsCount) {
  if (typeof emailsCount === "number") return emailsCount;

  if (emailsCount && typeof emailsCount === "object") {
    if (typeof emailsCount.total === "number") return emailsCount.total;

    if (
      typeof emailsCount.personal === "number" ||
      typeof emailsCount.generic === "number"
    ) {
      return (emailsCount.personal || 0) + (emailsCount.generic || 0);
    }
  }

  return null;
}

// ============================================================
// EXTRACT EMAIL CANDIDATES
// ============================================================

function extractEmailCandidates(response) {
  const candidates = [];
  const seen = new Set();

  function walk(value, depth = 0) {
    if (!value || depth > 12) return;

    if (Array.isArray(value)) {
      for (const item of value) walk(item, depth + 1);
      return;
    }

    if (typeof value !== "object") return;

    const email = value.value || value.email || value.email_address;

    if (typeof email === "string" && isValidEmail(email)) {
      const normalized = email.trim().toLowerCase();

      if (!seen.has(normalized)) {
        seen.add(normalized);

        candidates.push({
          email: normalized,
          first_name: value.first_name || value.firstName || null,
          last_name: value.last_name || value.lastName || null,
          full_name:
            value.full_name ||
            value.name ||
            joinName(
              value.first_name || value.firstName,
              value.last_name || value.lastName
            ),
          job_title: value.position || value.job_title || value.title || null,
          email_status:
            value.verification?.status ||
            value.status ||
            value.email_status ||
            null,
          linkedin_url: value.linkedin || value.linkedin_url || null,
        });
      }
    }

    for (const child of Object.values(value)) {
      if (child && typeof child === "object") {
        walk(child, depth + 1);
      }
    }
  }

  walk(response);
  return candidates;
}

// ============================================================
// NORMALIZE COMPANY DOMAIN
// ============================================================

function getCompanyDomain(company) {
  let value =
    company.domain ||
    company.company_domain ||
    company.website ||
    company.company_website ||
    null;

  if (typeof value !== "string" || !value.trim()) {
    return null;
  }

  value = value.trim();

  try {
    if (!/^https?:\/\//i.test(value)) {
      value = `https://${value}`;
    }

    const hostname = new URL(value).hostname
      .toLowerCase()
      .replace(/^www\./, "");

    if (
      !hostname.includes(".") ||
      hostname === "localhost" ||
      hostname.includes(" ")
    ) {
      return null;
    }

    return hostname;
  } catch {
    return null;
  }
}

// ============================================================
// EMAIL VALIDATION
// ============================================================

function isValidEmail(email) {
  if (typeof email !== "string" || email.length > 254) {
    return false;
  }

  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.trim());
}

function isBadStatus(status) {
  const value = String(status || "").toLowerCase();
  return value === "invalid" || value === "disposable";
}

function joinName(first, last) {
  return [first, last].filter(Boolean).join(" ") || null;
}

// ============================================================
// LEAD RELEVANCE SCORING
// ============================================================

function scoreCandidate(candidate, filters) {
  let score = 0;

  const title = String(candidate.job_title || "").toLowerCase();
  const status = String(candidate.email_status || "").toLowerCase();

  if (status === "valid" || status === "verified") score += 10;
  if (status === "accept_all" || status === "webmail") score += 2;

  // Match against the recommended job titles from the task.
  for (const wanted of filters.jobTitles) {
    const needle = wanted.toLowerCase();

    if (title === needle) {
      score += 12;
    } else if (title.includes(needle) || needle.includes(title)) {
      if (title) score += 8;
    }
  }

  // Decision-maker keywords.
  const seniorWords = [
    "founder",
    "ceo",
    "chief",
    "owner",
    "head of",
    "director",
    "vice president",
    "vp",
  ];

  for (const word of seniorWords) {
    if (title.includes(word)) score += 2;
  }

  // Role keywords relevant to the niche.
  const roleWords = [
    "growth",
    "marketing",
    "sales",
    "business development",
    "partnerships",
    "revenue",
    "demand",
    "acquisition",
  ];

  const context = `${filters.niche} ${filters.jobTitles.join(" ")}`
    .toLowerCase();

  for (const word of roleWords) {
    if (title.includes(word)) {
      score += context.includes(word) ? 5 : 1;
    }
  }

  return score;
}

// ============================================================
// DUPLICATE EMAIL CHECK
// ============================================================

async function leadEmailExists(env, applicationId, email) {
  const query =
    "leads" +
    "?select=id" +
    `&application_id=eq.${encodeURIComponent(applicationId)}` +
    `&email=ilike.${encodeURIComponent(email)}` +
    "&limit=1";

  const result = await supabaseRequest(env, query);

  return Array.isArray(result.data) && result.data.length > 0;
}

// ============================================================
// BUILD LEAD ROW
// ============================================================

function buildLead(task, company, candidate) {
  const firstName = candidate.first_name || null;
  const lastName = candidate.last_name || null;

  return {
    application_id: task.application_id,
    user_id: task.user_id,
    task_id: String(task.id),

    first_name: firstName,
    last_name: lastName,
    full_name: candidate.full_name || joinName(firstName, lastName),

    email: candidate.email.trim().toLowerCase(),
    email_status: candidate.email_status || null,
    job_title: candidate.job_title || null,
    linkedin_url: candidate.linkedin_url || null,

    company_name: company.name || null,
    company_domain: company.domain || null,
    company_website: company.website || `https://${company.domain}`,

    company_industry: company.industry || null,
    company_size:
      company.size === null || company.size === undefined
        ? null
        : String(company.size),

    company_country: company.country || null,
    company_city: company.city || null,

    message: null,
    reply_status: "pending",
  };
}

// ============================================================
// INSERT LEAD
// ============================================================

async function insertLead(env, lead) {
  const result = await supabaseRequest(env, "leads", {
    method: "POST",
    headers: { Prefer: "return=representation" },
    body: JSON.stringify(lead),
  });

  return Array.isArray(result.data) && result.data.length > 0;
}

// ============================================================
// DECREMENT PENDING COUNT
//
// IMPORTANT:
// This is a basic read-and-update implementation.
// For strict production safety, move lead insertion and
// pending_count decrement into one atomic PostgreSQL function.
// ============================================================

async function decrementPendingCount(env, task) {
  const readQuery =
    "planner_runs" +
    "?select=id,pending_count" +
    `&id=eq.${encodeURIComponent(task.id)}` +
    "&limit=1";

  const read = await supabaseRequest(env, readQuery);

  if (!Array.isArray(read.data) || !read.data.length) {
    throw new Error("Task not found while updating pending_count");
  }

  const currentCount = Number(read.data[0].pending_count || 0);
  const nextCount = Math.max(0, currentCount - 1);
  const nextStatus = nextCount === 0 ? "Completed" : "pending";

  const updateQuery =
    "planner_runs" + `?id=eq.${encodeURIComponent(task.id)}`;

  const updated = await supabaseRequest(env, updateQuery, {
    method: "PATCH",
    headers: { Prefer: "return=representation" },
    body: JSON.stringify({
      pending_count: nextCount,
      status: nextStatus,
      error: null,
    }),
  });

  if (!Array.isArray(updated.data) || !updated.data.length) {
    throw new Error("Could not update pending_count");
  }

  return updated.data[0];
}

// ============================================================
// JSON RESPONSE
// ============================================================

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}
