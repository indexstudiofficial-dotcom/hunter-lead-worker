// ============================================================
// HUNTER LEAD WORKER
//
// Trigger: Supabase Cron sends POST
// No CRON_SECRET
//
// Workflow:
// 1. Find oldest pending outreach task.
// 2. Claim the task.
// 3. Discover matching companies using Hunter.
// 4. Perform at most ONE paid Domain Search.
// 5. Select one suitable email.
// 6. Avoid duplicate leads.
// 7. Save one lead to Supabase.
// 8. Decrease pending_count after a new lead is saved.
// 9. Mark task Completed when pending_count reaches zero.
//
// Required Cloudflare secrets:
// SUPABASE_URL
// SUPABASE_SERVICE_ROLE_KEY
// COMPOSIO_API_KEY
//
// Optional variable:
// COMPOSIO_HUNTER_VERSION
// ============================================================

const COMPOSIO_BASE_URL = "https://backend.composio.dev/api/v3.1";
const DEFAULT_HUNTER_VERSION = "20261018_00";
const MAX_COMPANIES_TO_CHECK = 5;

// ============================================================
// HTTP ENTRY POINT
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

      return json(result);
    } catch (error) {
      console.error("Worker failed:", error.message);

      return json({
        ok: false,
        error: error.message,
      }, 500);
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

  return {
    data,
    headers: response.headers,
  };
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
// FIND AND SAVE ONE LEAD
// ============================================================

async function findAndSaveOneLead(env, task) {
  // Free discovery step.
  const companies = await discoverCompanies(env, task);

  if (!companies.length) {
    return {
      saved: false,
      reason: "Hunter discovery returned no usable companies",
    };
  }

  let company = null;

  for (const item of companies.slice(0, MAX_COMPANIES_TO_CHECK)) {
    const domain = getCompanyDomain(item);

    if (domain) {
      company = {
        ...item,
        domain,
      };
      break;
    }
  }

  if (!company) {
    return {
      saved: false,
      reason: "No discovered company had a usable domain",
    };
  }

  // Exactly one paid Domain Search call at most per worker run.
  const domainResult = await searchDomain(env, company.domain, task);

  const candidates = extractEmailCandidates(domainResult)
    .filter((candidate) => isValidEmail(candidate.email))
    .sort((a, b) => scoreCandidate(b, task) - scoreCandidate(a, task));

  if (!candidates.length) {
    return {
      saved: false,
      reason: "Domain Search returned no usable email addresses",
    };
  }

  for (const candidate of candidates) {
    const email = candidate.email.trim().toLowerCase();

    if (await leadEmailExists(env, task.application_id, email)) {
      continue;
    }

    const lead = buildLead(task, company, {
      ...candidate,
      email,
    });

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
    reason: "All returned email addresses were duplicates",
  };
}

// ============================================================
// HUNTER COMPANY DISCOVERY
//
// FIX:
// Do not pass a free-form industry filter to Hunter.
// Ask for niche-relevant companies without specifying an
// arbitrary industry value.
//
// NOTE:
// This uses Composio's text execution mode. If your exact
// Composio tool requires structured arguments, its schema
// must be used instead.
// ============================================================

async function discoverCompanies(env, task) {
  const niche = String(task.niche || "B2B companies").slice(0, 300);
  const taskInstructions = String(task.task || "").slice(0, 5000);

  const prompt = `
Discover companies relevant to this business niche.

Target niche:
${niche}

Additional requirements:
${taskInstructions}

Return available company information, including:
- Company name
- Company domain
- Website
- Industry
- Employee count
- Country
- City

Important:
- Find real companies only.
- Do not invent company names or domains.
- Do not use an unsupported or invented industry filter.
- Treat the niche as a search description, not an industry enum.
- Prefer companies matching the country, size, and audience
  described in the task.
- Return structured company results if available.
`;

  const response = await executeHunterTool(
    env,
    "HUNTER_DISCOVER_COMPANIES",
    prompt
  );

  return extractCompanies(response);
}

// ============================================================
// HUNTER DOMAIN SEARCH
// ============================================================

async function searchDomain(env, domain, task) {
  const prompt = `
Find professional email addresses for people at this company.

Company domain:
${domain}

Target niche:
${String(task.niche || "").slice(0, 300)}

Requirements:
${String(task.task || "").slice(0, 3000)}

Return available information:
- Email address
- First name
- Last name
- Full name
- Job title
- Email verification status
- LinkedIn URL, if available

Prefer people whose roles match the target audience.
Only return information supplied by Hunter.
Do not invent email addresses.
`;

  return executeHunterTool(
    env,
    "HUNTER_DOMAIN_SEARCH",
    prompt
  );
}

// ============================================================
// COMPOSIO EXECUTION
// ============================================================

async function executeHunterTool(env, toolSlug, text) {
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
      `Composio ${toolSlug} error: ` +
      JSON.stringify(data.error).slice(0, 1200)
    );
  }

  return data;
}

// ============================================================
// EXTRACT COMPANIES
// ============================================================

function extractCompanies(response) {
  const companies = [];
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

    const name =
      value.company_name ||
      value.company ||
      value.organization_name ||
      value.name;

    const domain = getCompanyDomain(value);

    if (name && domain) {
      if (!seen.has(domain)) {
        seen.add(domain);

        companies.push({
          name: String(name),
          domain,
          website:
            value.website ||
            value.company_website ||
            null,
          industry:
            value.industry ||
            value.company_industry ||
            null,
          size:
            value.employee_count ??
            value.company_size ??
            value.employees ??
            null,
          country:
            value.country ||
            value.company_country ||
            null,
          city:
            value.city ||
            value.company_city ||
            null,
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
  return companies;
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
      for (const item of value) {
        walk(item, depth + 1);
      }
      return;
    }

    if (typeof value !== "object") return;

    const email =
      value.value ||
      value.email ||
      value.email_address;

    if (typeof email === "string" && isValidEmail(email)) {
      const normalized = email.trim().toLowerCase();

      if (!seen.has(normalized)) {
        seen.add(normalized);

        candidates.push({
          email: normalized,
          first_name:
            value.first_name ||
            value.firstName ||
            null,
          last_name:
            value.last_name ||
            value.lastName ||
            null,
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

function joinName(first, last) {
  return [first, last].filter(Boolean).join(" ") || null;
}

// ============================================================
// SIMPLE LEAD RELEVANCE SCORING
// ============================================================

function scoreCandidate(candidate, task) {
  let score = 0;

  const title = String(candidate.job_title || "").toLowerCase();
  const status = String(candidate.email_status || "").toLowerCase();
  const taskText = String(task.task || "").toLowerCase();
  const niche = String(task.niche || "").toLowerCase();

  if (status === "valid" || status === "verified") {
    score += 10;
  }

  if (status === "invalid" || status === "disposable") {
    score -= 30;
  }

  const titles = [
    "founder",
    "ceo",
    "chief",
    "owner",
    "head of",
    "director",
    "vice president",
    " vp ",
  ];

  for (const word of titles) {
    if (title.includes(word.trim())) {
      score += 2;
    }
  }

  const relevantWords = [
    "growth",
    "marketing",
    "sales",
    "business development",
    "partnerships",
    "revenue",
    "operations",
  ];

  for (const word of relevantWords) {
    if (
      title.includes(word) &&
      (taskText.includes(word) || niche.includes(word))
    ) {
      score += 5;
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
    full_name:
      candidate.full_name ||
      joinName(firstName, lastName),

    email: candidate.email.trim().toLowerCase(),
    email_status: candidate.email_status || null,
    job_title: candidate.job_title || null,
    linkedin_url: candidate.linkedin_url || null,

    company_name: company.name || null,
    company_domain: company.domain || null,
    company_website:
      company.website ||
      `https://${company.domain}`,

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
    headers: {
      Prefer: "return=representation",
    },
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

  const nextStatus = nextCount === 0
    ? "Completed"
    : "pending";

  const updateQuery =
    "planner_runs" +
    `?id=eq.${encodeURIComponent(task.id)}`;

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
