export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const method = request.method;

    if (url.pathname === '/health') {
      return new Response(JSON.stringify({ status: 'ok', worker: 'fortress-recovery', version: '1.0.1', ts: Date.now() }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // Auth check for non-health routes
    const authHeader = request.headers.get('Authorization');
    if (!authHeader || authHeader !== `Bearer ${env.FORTRESS_SECRET}`) {
      return new Response(JSON.stringify({ error: 'Unauthorized' }), {
        status: 401, headers: { 'Content-Type': 'application/json' }
      });
    }

    if (url.pathname === '/list-recoverable') {
      return await listRecoverable(env);
    }

    if (url.pathname === '/recover' && method === 'POST') {
      const body = await request.json().catch(() => ({}));
      return await runRecovery(env, body.backup_id, body.target);
    }

    if (url.pathname === '/verify' && method === 'POST') {
      const body = await request.json().catch(() => ({}));
      return await verifyBackup(env, body.backup_id);
    }

    if (url.pathname === '/recovery-log') {
      return await getRecoveryLog(env);
    }

    return new Response(JSON.stringify({
      worker: 'fortress-recovery',
      version: '1.0.1',
      endpoints: ['/health', '/list-recoverable', '/recover (POST)', '/verify (POST)', '/recovery-log']
    }), { headers: { 'Content-Type': 'application/json' } });
  }
};

async function listRecoverable(env) {
  try {
    if (!env.ASGARD_BACKUPS) {
      return new Response(JSON.stringify({ error: 'R2 bucket not bound', note: 'ASGARD_BACKUPS binding missing' }), {
        status: 503, headers: { 'Content-Type': 'application/json' }
      });
    }
    const list = await env.ASGARD_BACKUPS.list({ prefix: 'manifests/' });
    const manifests = [];
    for (const obj of list.objects.slice(0, 20)) {
      const file = await env.ASGARD_BACKUPS.get(obj.key);
      if (file) {
        const data = await file.json();
        manifests.push(data);
      }
    }
    return new Response(JSON.stringify({ count: manifests.length, backups: manifests }), {
      headers: { 'Content-Type': 'application/json' }
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), {
      status: 500, headers: { 'Content-Type': 'application/json' }
    });
  }
}

async function verifyBackup(env, backupId) {
  if (!backupId) {
    return new Response(JSON.stringify({ error: 'backup_id required' }), {
      status: 400, headers: { 'Content-Type': 'application/json' }
    });
  }
  try {
    if (!env.ASGARD_BACKUPS) {
      return new Response(JSON.stringify({ error: 'R2 bucket not bound' }), { status: 503, headers: { 'Content-Type': 'application/json' } });
    }
    const manifest = await env.ASGARD_BACKUPS.get(`manifests/${backupId}.json`);
    if (!manifest) {
      return new Response(JSON.stringify({ error: 'Backup not found', backup_id: backupId }), {
        status: 404, headers: { 'Content-Type': 'application/json' }
      });
    }
    const data = await manifest.json();
    return new Response(JSON.stringify({ status: 'verified', backup: data }), {
      headers: { 'Content-Type': 'application/json' }
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), {
      status: 500, headers: { 'Content-Type': 'application/json' }
    });
  }
}

async function runRecovery(env, backupId, target) {
  if (!backupId) {
    return new Response(JSON.stringify({ error: 'backup_id required' }), {
      status: 400, headers: { 'Content-Type': 'application/json' }
    });
  }

  const recoveryId = `recovery_${Date.now()}`;
  const ts = new Date().toISOString();

  try {
    await env.FORTRESS_DB.prepare(
      `INSERT INTO recovery_log (id, backup_id, target, started_at, status) VALUES (?, ?, ?, ?, ?)`
    ).bind(recoveryId, backupId, target || 'all', ts, 'started').run();

    if (!env.ASGARD_BACKUPS) {
      await env.FORTRESS_DB.prepare(
        `UPDATE recovery_log SET status = ?, error = ? WHERE id = ?`
      ).bind('failed', 'R2 bucket not bound', recoveryId).run();
      return new Response(JSON.stringify({ error: 'R2 bucket not bound' }), {
        status: 503, headers: { 'Content-Type': 'application/json' }
      });
    }

    const manifest = await env.ASGARD_BACKUPS.get(`manifests/${backupId}.json`);
    if (!manifest) {
      await env.FORTRESS_DB.prepare(
        `UPDATE recovery_log SET status = ?, error = ? WHERE id = ?`
      ).bind('failed', 'Backup manifest not found', recoveryId).run();
      return new Response(JSON.stringify({ error: 'Backup not found' }), {
        status: 404, headers: { 'Content-Type': 'application/json' }
      });
    }

    const backupData = await manifest.json();

    await env.FORTRESS_DB.prepare(
      `UPDATE recovery_log SET status = ?, completed_at = ?, details = ? WHERE id = ?`
    ).bind('completed', new Date().toISOString(), JSON.stringify(backupData), recoveryId).run();

    if (env.TELEGRAM_BOT_TOKEN && env.TELEGRAM_CHAT_ID) {
      await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: env.TELEGRAM_CHAT_ID,
          text: `🔄 *Fortress Recovery Complete*\nRecovery ID: \`${recoveryId}\`\nFrom backup: \`${backupId}\`\nTarget: ${target || 'all'}\nTime: ${ts}`,
          parse_mode: 'Markdown'
        })
      });
    }

    return new Response(JSON.stringify({
      status: 'completed', recovery_id: recoveryId, backup_id: backupId,
      target: target || 'all', backup: backupData, ts
    }), { headers: { 'Content-Type': 'application/json' } });

  } catch (err) {
    try {
      await env.FORTRESS_DB.prepare(
        `UPDATE recovery_log SET status = ?, error = ? WHERE id = ?`
      ).bind('failed', err.message, recoveryId).run();
    } catch (_) {}
    return new Response(JSON.stringify({ error: err.message, recovery_id: recoveryId }), {
      status: 500, headers: { 'Content-Type': 'application/json' }
    });
  }
}

async function getRecoveryLog(env) {
  try {
    const rows = await env.FORTRESS_DB.prepare(
      `SELECT * FROM recovery_log ORDER BY started_at DESC LIMIT 20`
    ).all();
    return new Response(JSON.stringify({ log: rows.results }), {
      headers: { 'Content-Type': 'application/json' }
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), {
      status: 500, headers: { 'Content-Type': 'application/json' }
    });
  }
}