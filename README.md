# NCAA Player Tracker

A private web app that tracks college baseball players for a sports agency. Every night it scrapes each player's most recent game stats and emails a box-score report to their assigned agent.

---

## Deploying to Railway (Recommended)

Railway is a hosting platform that runs the app 24/7 in the cloud so you don't have to leave a computer on. The free tier is enough to get started.

### What you'll need

1. A **Railway account** — sign up free at [railway.app](https://railway.app)
2. A **GitHub account** — Railway deploys directly from a GitHub repository
3. A **Gmail App Password** — a special password that lets the app send email on your behalf (see instructions below)
4. *(Optional)* A **ScraperAPI key** — only needed to scrape `stats.ncaa.org`. Sign up free at [scraperapi.com](https://scraperapi.com). Most schools use Sidearm Sports, which doesn't need ScraperAPI.

---

### Step 1 — Put the code on GitHub

1. Go to [github.com/new](https://github.com/new) and create a **private** repository named `ncaa-tracker`. Keep it private — it will hold your credentials.
2. On your computer, open Terminal and run these commands one at a time:

   ```
   cd "Desktop/NCAA Bot/agency-tracker"
   git init
   git add .
   git commit -m "Initial commit"
   git branch -M main
   git remote add origin https://github.com/YOUR_USERNAME/ncaa-tracker.git
   git push -u origin main
   ```

   Replace `YOUR_USERNAME` with your GitHub username. If asked to log in, use your GitHub credentials.

---

### Step 2 — Create a Railway project

1. Go to [railway.app](https://railway.app) and click **New Project**.
2. Choose **Deploy from GitHub repo**.
3. Authorize Railway to access your GitHub account when prompted.
4. Select your `ncaa-tracker` repository.
5. Railway detects the `Procfile` automatically and starts building. Wait about 1–2 minutes for the first deploy to finish.

---

### Step 3 — Add a Volume (persistent database)

Without this step, your player and agent data will be erased every time Railway redeploys.

1. Inside your Railway project, click **+ New** and select **Volume**.
2. Name it `tracker-data` and click **Create**.
3. Click on the new volume, open the **Mount** tab, and set the mount path to `/data`.
4. Make sure the volume is attached to your web service (Railway usually does this automatically).

---

### Step 4 — Set environment variables

1. Click on your web service in Railway, then open the **Variables** tab.
2. Add each variable below using **+ New Variable**:

   | Variable | What to put | Notes |
   |---|---|---|
   | `GMAIL_USER` | `you@gmail.com` | The Gmail address that sends reports |
   | `GMAIL_APP_PASSWORD` | `xxxx xxxx xxxx xxxx` | See "Getting a Gmail App Password" below |
   | `SECRET_KEY` | *(long random string)* | Run `python3 -c "import secrets; print(secrets.token_hex(32))"` in Terminal to generate one |
   | `DATA_DIR` | `/data` | Tells the app to save its database to the Volume from Step 3 |
   | `EMAIL_FROM_NAME` | `NCAA Player Tracker` | The name shown in the email From field (can be anything) |
   | `SCRAPERAPI_KEY` | *(your key or blank)* | Optional — only needed for NCAA stats scraping |
   | `NIGHTLY_RUN_AT` | `23:00` | When to run the nightly job, in UTC 24-hour time. 23:00 UTC = 7 PM Eastern during summer |

3. After adding variables, Railway automatically redeploys. Wait for the green checkmark.

---

### Step 5 — Get your app's URL

1. Click on your web service, then the **Settings** tab.
2. Under **Networking**, click **Generate Domain**.
3. Railway gives you a public URL like `https://ncaa-tracker-production.up.railway.app`.
4. Open it — you should see the NCAA Player Tracker web interface.

**Bookmark this URL.** This is how you access the app going forward.

---

### Getting a Gmail App Password

An App Password is a separate password just for this app. Your regular Gmail password won't work.

1. Sign in to [myaccount.google.com](https://myaccount.google.com)
2. Click **Security** in the left sidebar
3. Under "How you sign in to Google", click **2-Step Verification** — turn it on if it isn't already
4. Back on the Security page, search for **App passwords** and click it
5. Under "App name", type `NCAA Tracker` and click **Create**
6. You'll see a 16-character password like `ekui icdf drzk avqb` — copy it now (you can't see it again)
7. Paste this as the value of `GMAIL_APP_PASSWORD` in Railway

---

## Using the app

### Adding an agent

1. Click **Agents** in the top menu
2. Enter the agent's name and email address, then click **Add Agent**

### Adding a player

1. Click **Players** → **Add Player**
2. Enter the player's name and school — for example, "Dax Whitney" and "Oregon State"
3. Select Hitter or Pitcher
4. Assign them to an agent
5. Leave the NCAA Stats URL blank and click **Add Player** — the app finds their stats page automatically

If auto-detection fails (the app will tell you why), enter the player's NCAA Stats URL manually:
- Go to [stats.ncaa.org](https://stats.ncaa.org), search for the player by name, open their profile, and paste the URL

### Triggering the nightly job manually

To send a test email right now without waiting for the scheduled time:

1. In Railway, open your web service and go to the **Deploy** tab
2. Scroll to the bottom and find the deploy for your latest commit
3. Click the three-dot menu next to it and select **Restart** — this restarts the app but doesn't run the job

For a real manual trigger, open a Terminal on your local machine (with `.env` configured) and run:

```
python scheduler.py --run-now
```

Or to back-fill a specific past date:

```
python scheduler.py --date 2026-04-18
```

---

## Environment variable reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `GMAIL_USER` | **Yes** | — | Gmail address used to send emails |
| `GMAIL_APP_PASSWORD` | **Yes** | — | Gmail App Password (not your account password) |
| `SECRET_KEY` | **Yes** | insecure dev key | Random string for Flask session signing |
| `DATA_DIR` | **Yes in prod** | project folder | Directory where `tracker.db` lives — set to `/data` on Railway |
| `EMAIL_FROM_NAME` | No | `NCAA Player Tracker` | Display name in email From field |
| `SCRAPERAPI_KEY` | No | *(blank)* | ScraperAPI key for scraping stats.ncaa.org |
| `SCRAPERAPI_ULTRA` | No | `false` | Set `true` if your plan includes Ultra Premium |
| `NIGHTLY_RUN_AT` | No | `23:00` | UTC time for the nightly job (HH:MM, 24-hour) |
| `LOG_FILE` | No | *(stdout only)* | Write logs to a file (Railway captures stdout automatically) |
| `FLASK_DEBUG` | No | `0` | Set to `1` for debug mode — **never in production** |
| `PORT` | Set by Railway | `5050` | HTTP port — Railway sets this automatically, don't change it |

---

## Running locally (for development)

```bash
# Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate        # Mac/Linux
# venv\Scripts\activate         # Windows

# Install dependencies
pip install -r requirements.txt

# Copy the example env file and fill in your values
cp .env.example .env
# Edit .env with your Gmail credentials etc.

# Start the web app
python app.py
# Open http://localhost:5050

# Run the nightly job right now to test scraping + email
python scheduler.py --run-now
```

---

## Troubleshooting

**The app won't start / Railway deploy fails**
- Open the **Logs** tab in your Railway service and look for Python errors
- Make sure all required environment variables are set: `GMAIL_USER`, `GMAIL_APP_PASSWORD`, `SECRET_KEY`, `DATA_DIR`

**Players and agents disappeared after a redeploy**
- The Volume isn't set up correctly. Re-do Step 3 and confirm `DATA_DIR=/data` is set in your Variables.

**Emails aren't being sent**
- Check Railway logs around your `NIGHTLY_RUN_AT` time for lines starting with `Nightly job`
- Confirm `GMAIL_USER` and `GMAIL_APP_PASSWORD` are set and contain no extra spaces
- Make sure you're using an App Password, not your regular Gmail password
- Confirm 2-Step Verification is enabled on the Gmail account

**Auto-detection fails for a player**
- The player's school may not be in the built-in school map yet
- Enter the NCAA Stats URL manually as a fallback: find the player at [stats.ncaa.org](https://stats.ncaa.org) and paste their profile URL

**A player shows "No game to report" every day**
- Check that the player's stats URL is correct — open it in a browser and confirm it's their profile page
- Check the **Game Logs** page to see the last date stats were captured

---

## File overview

| File | Purpose |
|---|---|
| `app.py` | Flask web UI + background scheduler thread |
| `scheduler.py` | Nightly job logic (scrape → email); also runnable standalone |
| `scraper.py` | Stats scrapers for Sidearm Nextgen, Sidearm Legacy, and NCAA |
| `platform_detector.py` | Finds a player's stats URL automatically from their name and school |
| `emailer.py` | Formats and sends box-score emails via Gmail |
| `database.py` | SQLite queries for agents, players, and game logs |
| `Procfile` | Tells Railway how to start the app (gunicorn) |
| `railway.json` | Railway health-check and restart configuration |
| `.env.example` | Template for your environment variables |
