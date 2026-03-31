'use strict';

const readline = require('node:readline/promises');
const { stdin: input, stdout: output } = require('node:process');
const { google } = require('googleapis');

async function main() {
  const clientId = String(process.env.GOOGLE_OAUTH_CLIENT_ID || '').trim();
  const clientSecret = String(process.env.GOOGLE_OAUTH_CLIENT_SECRET || '').trim();
  const redirectUri = String(process.env.GOOGLE_OAUTH_REDIRECT_URI || 'http://localhost:8085/oauth2/callback').trim();

  if (!clientId || !clientSecret) {
    console.error('Missing env: GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET are required.');
    process.exit(1);
  }

  const oauth2 = new google.auth.OAuth2(clientId, clientSecret, redirectUri);
  const authUrl = oauth2.generateAuthUrl({
    access_type: 'offline',
    prompt: 'consent',
    scope: [
      'https://www.googleapis.com/auth/drive.file',
      'https://www.googleapis.com/auth/drive.metadata.readonly'
    ]
  });

  console.log('\n1) Open this URL in browser and login with your designated Drive uploader account:\n');
  console.log(authUrl);
  console.log('\n2) Copy the "code" from the redirect URL and paste below.\n');

  const rl = readline.createInterface({ input, output });
  const code = (await rl.question('Authorization code: ')).trim();
  rl.close();

  if (!code) {
    console.error('No authorization code provided.');
    process.exit(1);
  }

  const { tokens } = await oauth2.getToken(code);
  if (!tokens.refresh_token) {
    console.error('No refresh token returned. Re-run and ensure prompt=consent, access_type=offline.');
    process.exit(1);
  }

  console.log('\nSet this in backend env:\n');
  console.log(`GOOGLE_OAUTH_REFRESH_TOKEN=${tokens.refresh_token}`);
  console.log('\n(Keep this secret. Do not commit it to GitHub.)');
}

main().catch((err) => {
  console.error('Failed:', err && err.message ? err.message : String(err));
  process.exit(1);
});
