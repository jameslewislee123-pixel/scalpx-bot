require("dotenv").config();
const { Client, GatewayIntentBits, REST, Routes, SlashCommandBuilder } = require("discord.js");
const sqlite3 = require("sqlite3").verbose();
const cron = require("node-cron");
const { google } = require("googleapis");

// ================= CONFIG =================
const ONE_MONTH = 30 * 24 * 60 * 60 * 1000;
const LIFETIME_UNTIL = 32503680000000; // Year 3000

const SHEET_TAB_NAME = process.env.SHEET_TAB_NAME || "Responses";
const SHEET_RANGE = `${SHEET_TAB_NAME}!A:Z`;

const SYNC_CRON = "*/5 * * * *"; // every 5 minutes

// ================= DATABASE =================
const db = new sqlite3.Database("./db.sqlite");

// Ensure schema
db.run(`
  CREATE TABLE IF NOT EXISTS vip_users (
    user_id TEXT PRIMARY KEY,
    vip_until INTEGER,
    trial_until INTEGER
  )
`);

// Migrate older installs (ignore errors)
db.run(`ALTER TABLE vip_users ADD COLUMN vip_until INTEGER`, () => {});
db.run(`ALTER TABLE vip_users ADD COLUMN trial_until INTEGER`, () => {});

// Promise helpers
function dbGet(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => (err ? reject(err) : resolve(row)));
  });
}
function dbRun(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}
function dbAll(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => (err ? reject(err) : resolve(rows)));
  });
}

// ================= GOOGLE SHEETS =================
async function getSheetRows() {
  const auth = new google.auth.GoogleAuth({
    keyFile: "credentials.json",
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
  });

  const sheets = google.sheets({ version: "v4", auth });
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.SHEET_ID,
    range: SHEET_RANGE,
  });

  return res.data.values || [];
}

// Returns a Map: discord_id -> approved(boolean)
async function getEligibilityMap() {
  const rows = await getSheetRows();
  if (rows.length < 2) return new Map();

  const headers = rows[0].map(h => (h || "").toString().toLowerCase().trim());
  const discordIdx = headers.indexOf("discord_id");
  const lifetimeIdx = headers.indexOf("lifetime_vip");

  if (discordIdx === -1) throw new Error("discord_id column not found in sheet headers");
  if (lifetimeIdx === -1) throw new Error("lifetime_vip column not found in sheet headers");

  const map = new Map();
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    const id = (row[discordIdx] || "").toString().trim();
    if (!id) continue;

    const val = (row[lifetimeIdx] || "").toString().trim().toUpperCase();
    const approved = val === "YES";
    map.set(id, approved);
  }
  return map;
}

// ================= DISCORD =================
const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMembers],
});

async function getVipRole(guild) {
  const role = guild.roles.cache.get(process.env.VIP_ROLE_ID);
  if (!role) throw new Error("VIP role not found. Check VIP_ROLE_ID and role hierarchy.");
  return role;
}

// Fetch one member by ID (no full guild fetch). Returns null if not found.
async function fetchMemberSafe(guild, userId) {
  try {
    return await guild.members.fetch(userId);
  } catch {
    return null;
  }
}

// Ensure trial_until exists for a member; uses joinedTimestamp fallback.
async function ensureTrialRecordFor(member) {
  const row = await dbGet(`SELECT vip_until, trial_until FROM vip_users WHERE user_id=?`, [member.id]).catch(() => null);

  const joinedTs = member.joinedTimestamp || Date.now();
  const fallbackTrialUntil = joinedTs + ONE_MONTH;

  if (!row) {
    await dbRun(
      `INSERT OR REPLACE INTO vip_users (user_id, vip_until, trial_until) VALUES (?, ?, ?)`,
      [member.id, fallbackTrialUntil, fallbackTrialUntil]
    );
    return { vip_until: fallbackTrialUntil, trial_until: fallbackTrialUntil };
  }

  if (!row.trial_until || row.trial_until === 0) {
    let trialUntil = fallbackTrialUntil;
    if (row.vip_until && row.vip_until > Date.now() && row.vip_until < LIFETIME_UNTIL) {
      trialUntil = row.vip_until;
    }
    await dbRun(`UPDATE vip_users SET trial_until=? WHERE user_id=?`, [trialUntil, member.id]);
    return { vip_until: row.vip_until || 0, trial_until: trialUntil };
  }

  return { vip_until: row.vip_until || 0, trial_until: row.trial_until || 0 };
}

async function grantTrialOnJoin(member) {
  const role = await getVipRole(member.guild);

  const trialUntil = Date.now() + ONE_MONTH;
  await member.roles.add(role);

  await dbRun(
    `INSERT OR REPLACE INTO vip_users (user_id, vip_until, trial_until) VALUES (?, ?, ?)`,
    [member.id, trialUntil, trialUntil]
  );

  return trialUntil;
}

async function setVipLifetime(member) {
  const role = await getVipRole(member.guild);
  await member.roles.add(role);

  const existing = await ensureTrialRecordFor(member);
  const trialUntil = existing?.trial_until || (Date.now() + ONE_MONTH);

  await dbRun(
    `INSERT OR REPLACE INTO vip_users (user_id, vip_until, trial_until) VALUES (?, ?, ?)`,
    [member.id, LIFETIME_UNTIL, trialUntil]
  );
}

async function setVipToTrialRemainingOrRemove(member) {
  const role = await getVipRole(member.guild);

  const existing = await ensureTrialRecordFor(member);
  const trialUntil = existing?.trial_until || 0;

  if (trialUntil > Date.now()) {
    // still in trial
    await member.roles.add(role);
    await dbRun(
      `INSERT OR REPLACE INTO vip_users (user_id, vip_until, trial_until) VALUES (?, ?, ?)`,
      [member.id, trialUntil, trialUntil]
    );
    return { keptTrial: true, trialUntil };
  }

  // trial expired -> remove VIP
  if (member.roles.cache.has(role.id)) {
    await member.roles.remove(role).catch(() => {});
  }

  await dbRun(
    `INSERT OR REPLACE INTO vip_users (user_id, vip_until, trial_until) VALUES (?, ?, ?)`,
    [member.id, 0, trialUntil]
  );

  return { keptTrial: false, trialUntil };
}

async function removeVipIfHas(member) {
  const role = await getVipRole(member.guild);
  if (member.roles.cache.has(role.id)) {
    await member.roles.remove(role).catch(() => {});
  }
}

// ================= OPTIMISED SYNC =================
// Lock to prevent overlapping sync runs
let syncRunning = false;

async function syncFromSheetOptimised() {
  if (syncRunning) return { skipped: true };
  syncRunning = true;

  const started = Date.now();
  try {
    const guild = await client.guilds.fetch(process.env.GUILD_ID);

    // Read eligibility from sheet
    const eligibility = await getEligibilityMap();

    // Read DB users once
    const dbRows = await dbAll(`SELECT user_id, vip_until, trial_until FROM vip_users`);

    // Build sets for fast lookup
    const lifetimeInDb = new Set(
      dbRows.filter(r => (r.vip_until || 0) >= LIFETIME_UNTIL).map(r => r.user_id)
    );

    // 1) Apply YES: ensure lifetime VIP for approved users
    // Only fetch users by ID; do not fetch whole guild
    let lifetimeEnsured = 0;
    for (const [userId, approved] of eligibility.entries()) {
      if (!approved) continue;

      const member = await fetchMemberSafe(guild, userId);
      if (!member) continue;

      await setVipLifetime(member);
      lifetimeEnsured++;
    }

    // 2) Downgrade DB-lifetime users who are not approved anymore (NO or not present)
    let lifetimeDowngraded = 0;
    for (const userId of lifetimeInDb) {
      const approved = eligibility.get(userId) === true;
      if (approved) continue;

      const member = await fetchMemberSafe(guild, userId);
      if (!member) continue;

      await setVipToTrialRemainingOrRemove(member);
      lifetimeDowngraded++;
    }

    // 3) Remove expired trials (doesn't require sheet)
    // Only checks users who are in DB and have an expiry in the past.
    let expiredRemoved = 0;
    for (const r of dbRows) {
      const vipUntil = r.vip_until || 0;
      if (!vipUntil) continue;
      if (vipUntil >= LIFETIME_UNTIL) continue;
      if (vipUntil > Date.now()) continue;

      const member = await fetchMemberSafe(guild, r.user_id);
      if (!member) continue;

      await removeVipIfHas(member);
      expiredRemoved++;
    }

    const ms = Date.now() - started;
    return { skipped: false, lifetimeEnsured, lifetimeDowngraded, expiredRemoved, ms, totalSheet: eligibility.size };
  } finally {
    syncRunning = false;
  }
}

// ================= EVENTS =================
client.once("ready", async () => {
  console.log(`Logged in as ${client.user.tag}`);

  const commands = [
    new SlashCommandBuilder().setName("form").setDescription("Get the VIP verification form"),
    new SlashCommandBuilder().setName("status").setDescription("Check your VIP status"),
    new SlashCommandBuilder().setName("reactivate").setDescription("Sync your VIP status now (optional)"),
  ];

  const rest = new REST({ version: "10" }).setToken(process.env.DISCORD_TOKEN);
  await rest.put(
    Routes.applicationGuildCommands(client.user.id, process.env.GUILD_ID),
    { body: commands.map(c => c.toJSON()) }
  );

  // Initial sync on startup
  try {
    const r = await syncFromSheetOptimised();
    console.log("Initial sync:", r);
  } catch (e) {
    console.error("Initial sync failed:", e);
  }
});

// 30-day trial on join
client.on("guildMemberAdd", async member => {
  try {
    const until = await grantTrialOnJoin(member);
    const ch = member.guild.channels.cache.get(process.env.LOG_CHANNEL_ID);
    if (ch) ch.send(`✅ Gave 30-day VIP trial to <@${member.id}> until <t:${Math.floor(until / 1000)}:F>`);
  } catch (e) {
    console.error("guildMemberAdd error:", e);
  }
});

client.on("interactionCreate", async interaction => {
  if (!interaction.isChatInputCommand()) return;
  const member = await interaction.guild.members.fetch(interaction.user.id);

  if (interaction.commandName === "form") {
    return interaction.reply({ content: process.env.FORM_URL, ephemeral: true });
  }

  if (interaction.commandName === "status") {
    try {
      const row = await dbGet(`SELECT vip_until FROM vip_users WHERE user_id=?`, [member.id]).catch(() => null);
      if (!row || !row.vip_until) return interaction.reply({ content: "❌ You do not currently have VIP.", ephemeral: true });

      if (row.vip_until >= LIFETIME_UNTIL) {
        return interaction.reply({ content: "✅ VIP status: **LIFETIME ACCESS**", ephemeral: true });
      }

      if (row.vip_until > Date.now()) {
        return interaction.reply({
          content: `✅ VIP active until <t:${Math.floor(row.vip_until / 1000)}:F>`,
          ephemeral: true
        });
      }

      return interaction.reply({ content: "❌ VIP expired.", ephemeral: true });
    } catch (e) {
      console.error(e);
      return interaction.reply({ content: "❌ Status error.", ephemeral: true });
    }
  }

  // Manual "sync now" (optional)
  if (interaction.commandName === "reactivate") {
    await interaction.deferReply({ ephemeral: true });
    try {
      const r = await syncFromSheetOptimised();
      if (r.skipped) return interaction.editReply("⏳ Sync already running — try again in a moment.");
      return interaction.editReply("✅ Synced. Your VIP status is up to date.");
    } catch (e) {
      console.error(e);
      return interaction.editReply(`❌ Sync error: ${e.message}`);
    }
  }
});

// ================= CRON =================
// Optimised full sync every 5 minutes
cron.schedule(SYNC_CRON, async () => {
  try {
    const r = await syncFromSheetOptimised();
    if (!r.skipped) console.log("5-min sync:", r);
  } catch (e) {
    console.error("5-min sync failed:", e);
  }
});

// Optional: extra hourly cleanup (safe redundancy)
cron.schedule("0 * * * *", async () => {
  try {
    const guild = await client.guilds.fetch(process.env.GUILD_ID);
    const dbRows = await dbAll(`SELECT user_id, vip_until FROM vip_users`);
    for (const r of dbRows) {
      const vipUntil = r.vip_until || 0;
      if (!vipUntil) continue;
      if (vipUntil >= LIFETIME_UNTIL) continue;
      if (vipUntil > Date.now()) continue;

      const member = await fetchMemberSafe(guild, r.user_id);
      if (!member) continue;

      await removeVipIfHas(member);
    }
  } catch (e) {
    console.error("Hourly cleanup failed:", e);
  }
});

// ================= START =================
client.login(process.env.DISCORD_TOKEN);
