# EPOS WhatsApp gateway

Links Ekaksh's WhatsApp number to EPOS without the Meta Business API or any paid service.

- Inbound messages are posted to EPOS (`/api/whatsapp/gateway`), which turns enquiries into leads and asks the owner on WhatsApp what to reply.
- Outbound messages are read from the `WaOutbox` table and sent one at a time.
- The WhatsApp login is kept in the `WaAuth` table, so the number is scanned once.

## Deploy (Render, free plan)

1. Render → New → Blueprint → paste this repository's URL.
2. When asked, paste `DATABASE_URL` (the Supabase session-pooler URL EPOS uses) and `GATEWAY_SECRET` (the same value as `WA_GATEWAY_SECRET` on Vercel).
3. In EPOS: Integrations → WhatsApp → **Link Ekaksh's WhatsApp number** → **Show QR code** → scan it from the business phone (WhatsApp → Linked devices).
4. Enter the owner's number under "Owner's WhatsApp" and save.

The service keeps itself awake through a Supabase `pg_cron` job that pings `/health` every 10 minutes.
