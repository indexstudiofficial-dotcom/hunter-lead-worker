// ============================================================
// Trigger: Supabase Cron POST request
// Authentication: No CRON_SECRET
// ============================================================

const COMPOSIO_BASE_URL = "https://backend.composio.dev/api/v3.1";

const HUNTER_VERSION = "20261018_00";

const MAX_COMPANIES_TO_CHECK = 5;

// ============================================================
// MAIN REQUEST HANDLER
// ============================================================

export default {
  async fetch(request, env) {
    if (request.method !== "POST") {
      return json({
        ok: false,
        error: "POST requests only",
      }, 405);
    }

    try {
      validateEnvironment(env);

      const result = await processOneTask(env);

      return json(result, 200);
    } catch (error) {
      console.error("Worker error:", error.message);

      return json({
        ok: false,
        error: error.message,
      }, 500);
    }
  },
};

// ============================================================
// ENVIRONMENT VALIDATION
// ============================================================

function validateEnvironment(env) {
  const required = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "COMPOSIO_API_KEY",
  ];

  for (const key of required) {
    if (!env[key]) {
      throw new Error(`Missing environment variable: ${key}`);
    }
  }
}

// ============================================================
// MAIN TASK PROCESSOR
// ============================================================

async function processOneTask(env) {
  // Find the oldest pending outreach task.
  const task = await findOldestPendingTask(env);

  if (!task) {
    return {
      ok: true,
      message: "No pending outreach tasks",
      processed: false,
    };
  }

  // Attempt to claim the task.
  // Only one request should successfully change pending to Processing.
  const claimed = await claimTask(env, task);

  if (!claimed) {
    return {
      ok: true,
      message: "Task was already claimed by another worker",
      processed: false,
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

    // Decrease pending_count only after a successful lead insert.
    const updatedTask = await decrementPendingCount(env, task);

    return {
      ok: true,
      processed: true,
      task_id: task.id,
      lead: result.lead,
      pending_count: updatedTask.pending_count,
      task_status: updatedTask.status,
    };
  } catch (error) {
    // Leave the task retryable after a failure.
    await releaseTask(env, task.id, error.message);

    throw error;
  }
}

// ============================================================
// SUPABASE REST HELPERS
// ============================================================

async function supabaseRequest(env, path, options = {}) {
  const baseUrl = env.SUPABASE_URL.replace(/\/+$/, "");

  const response = await fetch(`${baseUrl}/rest/v1/${path}`, {
    ...options,

    headers: {
      apikey: env.SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
      ...options.headers,
    },
  });

  const text = await response.text();

  let data = null;

  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }

  if (!response.ok) {
    throw new Error(
      `Supabase error ${response.status}: ${JSON.stringify(data)}`
    );
  }

  return {
    data,
    headers: response.headers,
  };
}

// ============================================================
// FIND OLDEST PENDING TASK
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
    `planner_runs?id=eq.${encodeURIComponent(task.id)}` +
    "&status=eq.pending";

  const result = await supabaseRequest(env, query, {
    method: "PATCH",

    headers: {
      Prefer: "return=representation",
    },

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
    `planner_runs?id=eq.${encodeURIComponent(taskId)}` +
    "&status=eq.Processing";

  await supabaseRequest(env, query, {
    method: "PATCH",

    body: JSON.stringify({
      status: "pending",
      error: String(reason).slice(0, 2000),
    }),
  });
}

// ============================================================
// DISCOVER COMPANIES AND FIND ONE LEAD
// ============================================================

async function findAndSaveOneLead(env, task) {
  // Step 1: Free Hunter company discovery.
  const companies = await discoverCompanies(env, task);

  if (companies.length === 0) {
    return {
      saved: false,
      reason: "No matching companies found",
    };
  }

  // Step 2: Try up to five companies.
  // A maximum of ONE paid Domain Search is allowed per run.
  //
  // We only make the paid call for the first company that has
  // a usable domain. We do not make another paid search if it
  // returns no suitable email.
  let selectedCompany = null;

  for (const company of companies.slice(0, MAX_COMPANIES_TO_CHECK)) {
    const domain = getCompanyDomain(company);

    if (domain) {
      selectedCompany = {
        ...company,
        domain,
      };

      break;
    }
  }

  if (!selectedCompany) {
    return {
      saved: false,
      reason: "No company with a usable domain was found",
    };
  }

  // Step 3: One paid Domain Search at most.
  const domainResult = await searchDomain(
    env,
    selectedCompany.domain,
    task
  );

  // Step 4: Extract email candidates.
  const candidates = extractEmailCandidates(domainResult);

  if (candidates.length === 0) {
    return {
      saved: false,
      reason: "Domain Search returned no usable email candidates",
    };
  }

  // Step 5: Rank candidates for the niche.
  const rankedCandidates = candidates
    .filter((candidate) => isValidEmail(candidate.email))
    .sort((a, b) => scoreCandidate(b, task) - scoreCandidate(a, task));

  // Step 6: Save the first suitable, non-duplicate lead.
  for (const candidate of rankedCandidates) {
    const email = candidate.email.trim().toLowerCase();

    const exists = await leadEmailExists(
      env,
      task.application_id,
      email
    );

    if (exists) {
      continue;
    }

    const lead = buildLead(task, selectedCompany, candidate);

    const inserted = await insertLead(env, lead);

    if (inserted) {
      return {
        saved: true,
        lead: {
          email: lead.email,
          full_name: lead.full_name,
          company_name: lead.company_name,
          company_domain: lead.company_domain,
        },
      };
    }
  }

  return {
    saved: false,
    reason: "No new suitable email was found",
  };
}

// ============================================================
// HUNTER: COMPANY DISCOVERY
// ============================================================

async function discoverCompanies(env, task) {
  const prompt = `
Find companies matching this lead-generation task.

TASK:
${task.task || ""}

TARGET NICHE:
${task.niche || ""}

Requirements:
- Prefer companies that match the requested industry and niche.
- Respect the requested country and company size when possible.
- Return companies with their name, domain, website, industry,
  employee count, country, and city when available.
- Return structured company results.
- Do not invent companies or domains.
`;

  const response = await executeHunterTool(
    env,
    "HUNTER_DISCOVER_COMPANIES",
    prompt
  );

  return extractCompanies(response);
}

// ============================================================
// HUNTER: DOMAIN SEARCH
// ============================================================

async function searchDomain(env, domain, task) {
  const prompt = `
Search Hunter for professional email addresses at this company.

Company domain: ${domain}

Target niche:
${task.niche || ""}

Task requirements:
${task.task || ""}

Return available email candidates with:
- email
- first name
- last name
- full name
- job title
- email verification status
- LinkedIn URL if available

Prefer relevant decision-makers for the target niche.
Return only data supplied by Hunter. Do not invent email addresses.
`;

  return await executeHunterTool(
    env,
    "HUNTER_DOMAIN_SEARCH",
    prompt
  );
}

// ============================================================
// COMPOSIO INSTANT TOOL EXECUTION
// ============================================================

async function executeHunterTool(env, toolSlug, text) {
  const response = await fetch(
    `${COMPOSIO_BASE_URL}/tools/execute/${toolSlug}`,
    {
      method: "POST",

      headers: {
        "x-api-key": env.COMPOSIO_API_KEY,
        "Content-Type": "application/json",
      },

      body: JSON.stringify({
        connected_account_id: "instant_account",
        version: HUNTER_VERSION,
        text,
      }),
    }
  );

  const raw = await response.text();

  let data;

  try {
    data = raw ? JSON.parse(raw) : {};
  } catch {
    throw new Error(
      `Composio returned a non-JSON response for ${toolSlug}`
    );
  }

  if (!response.ok) {
    throw new Error(
      `Composio ${toolSlug} failed (${response.status}): ` +
      JSON.stringify(data).slice(0, 1500)
    );
  }

  if (data.error) {
    throw new Error(
      `Composio ${toolSlug} error: ${JSON.stringify(data.error)}`
    );
  }

  return data;
}

// ============================================================
// EXTRACT COMPANIES FROM COMPOSIO RESPONSE
// ============================================================

function extractCompanies(response) {
  const results = [];
  const seen = new Set();

  function walk(value, depth = 0) {
    if (!value || depth > 8) return;

    if (Array.isArray(value)) {
      for (const item of value) {
        walk(item, depth + 1);
      }

      return;
    }

    if (typeof value !== "object") return;

    const domain = getCompanyDomain(value);
    const name =
      value.company_name ||
      value.company ||
      value.name ||
      value.organization_name;

    if (domain && name) {
      const key = domain.toLowerCase();

      if (!seen.has(key)) {
        seen.add(key);

        results.push({
          name: String(name),
          domain,
          website: value.website || value.company_website || null,
          industry: value.industry || value.company_industry || null,
          size:
            value.employee_count ||
            value.company_size ||
            value.employees ||
            null,
          country: value.country || value.company_country || null,
          city: value.city || value.company_city || null,
        });
      }
    }

    for (const [key, child] of Object.entries(value)) {
      if (typeof child === "object" && child !== null) {
        walk(child, depth + 1);
      }
    }
  }

  walk(response);

  return results;
}

// ============================================================
// EXTRACT EMAIL CANDIDATES FROM HUNTER RESPONSE
// ============================================================

function extractEmailCandidates(response) {
  const results = [];
  const seen = new Set();

  function walk(value, depth = 0) {
    if (!value || depth > 10) return;

    if (Array.isArray(value)) {
      for (const item of value) {
        walk(item, depth + 1);
      }

      return;
    }

    if (typeof value !== "object") return;

    const possibleEmail =
      value.value ||
      value.email ||
      value.email_address;

    if (
      typeof possibleEmail === "string" &&
      isValidEmail(possibleEmail)
    ) {
      const email = possibleEmail.trim().toLowerCase();

      if (!seen.has(email)) {
        seen.add(email);

        results.push({
          email,
          first_name: value.first_name || value.firstName || null,
          last_name: value.last_name || value.lastName || null,
          full_name:
            value.full_name ||
            value.name ||
            joinName(value.first_name, value.last_name),
          job_title:
            value.position ||
            value.job_title ||
            value.title ||
            null,
          email_status:
            value.verification?.status ||
            value.status ||
            value.email_status ||
            null,
          linkedin_url:
            value.linkedin ||
            value.linkedin_url ||
            null,
        });
      }
    }

    for (const child of Object.values(value)) {
      if (typeof child === "object" && child !== null) {
        walk(child, depth + 1);
      }
    }
  }

  walk(response);

  return results;
}

// ============================================================
// COMPANY DOMAIN HELPERS
// ============================================================

function getCompanyDomain(company) {
  let domain =
    company.domain ||
    company.company_domain ||
    company.website ||
    company.company_website ||
    null;

  if (!domain || typeof domain !== "string") {
    return null;
  }

  domain = domain.trim();

  if (!domain) return null;

  try {
    if (!domain.startsWith("http://") &&
        !domain.startsWith("https://")) {
      domain = `https://${domain}`;
    }

    const url = new URL(domain);
    const hostname = url.hostname.toLowerCase();

    if (
      !hostname.includes(".") ||
      hostname === "localhost" ||
      hostname.includes(" ")
    ) {
      return null;
    }

    return hostname.replace(/^www\./, "");
  } catch {
    return null;
  }
}

// ============================================================
// EMAIL HELPERS
// ============================================================

function isValidEmail(email) {
  if (typeof email !== "string") return false;

  const normalized = email.trim();

  if (normalized.length > 254) return false;

  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalized);
}

function joinName(first, last) {
  return [first, last].filter(Boolean).join(" ") || null;
}

// ============================================================
// CANDIDATE SCORING
// ============================================================

function scoreCandidate(candidate, task) {
  let score = 0;

  const title = String(candidate.job_title || "").toLowerCase();
  const emailStatus = String(
    candidate.email_status || ""
  ).toLowerCase();

  const taskText = String(task.task || "").toLowerCase();

  if (
    emailStatus.includes("valid") ||
    emailStatus.includes("verified")
  ) {
    score += 10;
  }

  if (
    emailStatus.includes("invalid") ||
    emailStatus.includes("disposable")
  ) {
    score -= 20;
  }

  const seniorityTerms = [
    "founder",
    "chief",
    "ceo",
    "owner",
    "president",
    "vice president",
    " vp ",
    "head of",
    "director",
  ];

  for (const term of seniorityTerms) {
    if (title.includes(term.trim())) {
      score += 3;
    }
  }

  // Add points if the job title relates to the task's niche.
  const relevantTerms = [
    "growth",
    "marketing",
    "sales",
    "business development",
    "partnerships",
    "revenue",
    "operations",
  ];

  for (const term of relevantTerms) {
    if (title.includes(term) && taskText.includes(term)) {
      score += 5;
    }
  }

  return score;
}

// ============================================================
// CHECK FOR DUPLICATE LEADS
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
// BUILD LEAD RECORD
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

    full_name:
      candidate.full_name ||
      joinName(firstName, lastName),

    email: candidate.email.trim().toLowerCase(),
    email_status: candidate.email_status,

    job_title: candidate.job_title,
    linkedin_url: candidate.linkedin_url,

    company_name: company.name,
    company_domain: company.domain,
    company_website:
      company.website ||
      `https://${company.domain}`,

    company_industry: company.industry,
    company_size:
      company.size === null || company.size === undefined
        ? null
        : String(company.size),

    company_country: company.country,
    company_city: company.city,

    // User requested no generated message yet.
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

    headers: {
      Prefer: "return=representation",
    },

    body: JSON.stringify(lead),
  });

  return Array.isArray(result.data) && result.data.length > 0;
}

// ============================================================
// DECREASE TASK PENDING COUNT
// ============================================================

async function decrementPendingCount(env, task) {
  // Re-read the latest task values before updating.
  const readQuery =
    "planner_runs" +
    "?select=id,pending_count,status" +
    `&id=eq.${encodeURIComponent(task.id)}` +
    "&limit=1";

  const read = await supabaseRequest(env, readQuery);

  if (!Array.isArray(read.data) || read.data.length === 0) {
    throw new Error("Task disappeared after lead insertion");
  }

  const current = read.data[0];
  const currentCount = Number(current.pending_count || 0);

  const nextCount = Math.max(0, currentCount - 1);

  const nextStatus = nextCount === 0
    ? "Completed"
    : "pending";

  const updateQuery =
    `planner_runs?id=eq.${encodeURIComponent(task.id)}`;

  const updated = await supabaseRequest(env, updateQuery, {
    method: "PATCH",

    headers: {
      Prefer: "return=representation",
    },

    body: JSON.stringify({
      pending_count: nextCount,
      status: nextStatus,
      error: null,
    }),
  });

  if (!Array.isArray(updated.data) || updated.data.length === 0) {
    throw new Error("Could not update task pending_count");
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
