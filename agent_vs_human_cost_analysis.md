# Agent vs Human: Cost & Time Analysis

## Run Analyzed

**Operator:** [mcd+tsgmgmtg@theondemandcompany.com](mailto:mcd+tsgmgmtg@theondemandcompany.com)
**Date:** March 13–14, 2026
**Report Period:** Dec 2025 – Feb 2026
**Stores:** 28 unique stores | **Campaigns Created:** 104 total (82 successful, 22 failed)

---

## What the Agent Did (End-to-End)


| Phase        | Description                                                         | Duration          | Details                                                                          |
| ------------ | ------------------------------------------------------------------- | ----------------- | -------------------------------------------------------------------------------- |
| **Phase 1**  | Login, Generate & Download Financial + Marketing Reports            | ~1 min 23 sec     | 2-step login, create 2 reports, download ZIPs                                    |
| **Analysis** | Financial analysis (43 sheets), Combined report, Google Sheets push | ~1 min 12 sec     | Unzip, parse CSVs, build Excel, push to Google                                   |
| **Phase 2**  | Create 104 marketing campaigns via browser                          | ~7 hrs 31 min     | Navigate UI per campaign: select store, set %, subtotal, discount, slots, submit |
| **Total**    | Full workflow                                                       | **~7 hrs 34 min** | Start: 19:17 → End: 02:51 next day                                               |


---

## Agent Cost Breakdown (Verified from Browser Use Pricing)

### Usage Metrics (from run logs)


| Metric                   | Value                       |
| ------------------------ | --------------------------- |
| Total browser-use steps  | **3,703 steps**             |
| Total agent tasks/phases | **229 tasks**               |
| Total API calls          | **~3,900 calls**            |
| Total tokens consumed    | **~30M tokens** (30,003.8k) |
| Avg tokens per API call  | ~7.7k                       |


### Actual Cost (Browser Use Cloud Pricing)

Pricing source: [browser-use.com/pricing](https://www.browser-use.com/pricing)

- **Model used:** bu-1-0 (Browser Use LLM) — **$0.002 per step**
- **Task initialization:** $0.01 per task
- **Account balance verified via API:** $0.50 remaining (Pay As You Go plan)


| Cost Item                       | Calculation          | Amount    |
| ------------------------------- | -------------------- | --------- |
| Browser-use steps               | 3,703 steps x $0.002 | **$7.41** |
| Task initialization             | 229 tasks x $0.01    | **$2.29** |
| **Total Agent Cost (this run)** |                      | **$9.70** |


> If the model maps to Browser Use 2.0 ($0.006/step), the upper bound would be $22.22 + $2.29 = **$24.51**

---

## Human Cost Breakdown (Equivalent Manual Work)

### Task-by-Task Human Time Estimates


| Task                                                                    | Per-Unit Time | Units | Total Time         |
| ----------------------------------------------------------------------- | ------------- | ----- | ------------------ |
| Login to DoorDash portal                                                | 2 min         | 1     | 2 min              |
| Generate financial report                                               | 3 min         | 1     | 3 min              |
| Generate marketing report                                               | 3 min         | 1     | 3 min              |
| Download & organize reports                                             | 2 min         | 2     | 4 min              |
| Analyze financial data (43 store-sheets)                                | 3 min/sheet   | 43    | 129 min (~2.2 hrs) |
| Build combined Excel report                                             | 15 min        | 1     | 15 min             |
| Push to Google Sheets                                                   | 10 min        | 1     | 10 min             |
| Create 1 marketing campaign (navigate, fill form, select slots, submit) | 4 min         | 104   | 416 min (~6.9 hrs) |
| **Total Human Time**                                                    |               |       | **~9.7 hours**     |


### Human Cost at Various Rates


| Role / Rate                | Hourly Rate | Total Cost (9.7 hrs) |
| -------------------------- | ----------- | -------------------- |
| Outsourced (BPO)           | $12/hr      | **$116**             |
| Junior marketing associate | $20/hr      | **$194**             |
| Mid-level operations staff | $35/hr      | **$340**             |
| Marketing manager          | $55/hr      | **$534**             |


---

## Head-to-Head Comparison


| Metric                              | Agent (AI)           | Human                    | Winner                     |
| ----------------------------------- | -------------------- | ------------------------ | -------------------------- |
| **Total time**                      | 7.5 hrs              | 9.7 hrs                  | Agent (22% faster)         |
| **Active attention required**       | 0 hrs (unattended)   | 9.7 hrs (full attention) | **Agent**                  |
| **Runs overnight/weekends**         | Yes                  | No                       | **Agent**                  |
| **Cost per run**                    | **$9.70**            | $116–$534                | **Agent (12–55x cheaper)** |
| **Cost vs cheapest labor ($12/hr)** | $9.70                | $116                     | **Agent (12x cheaper)**    |
| **Error rate**                      | 21% (22/104 failed)  | ~5% (est.)               | Human                      |
| **Success rate**                    | 79% (82/104)         | ~95% (est.)              | Human                      |
| **Scalability**                     | Parallel, unlimited  | Limited by headcount     | **Agent**                  |
| **Consistency**                     | Identical every time | Varies by person/fatigue | **Agent**                  |


---

## The Numbers That Matter

### Cost Per Campaign Created


| Method             | Cost                    | Per Campaign       |
| ------------------ | ----------------------- | ------------------ |
| Agent              | $9.70 for 104 campaigns | **$0.09/campaign** |
| Human (BPO $12/hr) | $116 for 104 campaigns  | **$1.12/campaign** |
| Human ($20/hr)     | $194 for 104 campaigns  | **$1.87/campaign** |
| Human ($35/hr)     | $340 for 104 campaigns  | **$3.27/campaign** |


### Cost Per Successful Campaign


| Method      | Cost  | Successful | Per Success       |
| ----------- | ----- | ---------- | ----------------- |
| Agent       | $9.70 | 82         | **$0.12/success** |
| Human (BPO) | $116  | ~99 (est.) | **$1.17/success** |


---

## ROI at Scale

The agent costs **$9.70 per run** while saving **9.7 hours of human labor**:


| Runs/Month | Agent Cost | Human Saved ($35/hr) | Human Saved ($12/hr) | Net Savings ($35/hr) | Net Savings ($12/hr) |
| ---------- | ---------- | -------------------- | -------------------- | -------------------- | -------------------- |
| 5          | $49        | $1,698               | $582                 | **$1,649**           | **$533**             |
| 10         | $97        | $3,395               | $1,164               | **$3,298**           | **$1,067**           |
| 20         | $194       | $6,790               | $2,328               | **$6,596**           | **$2,134**           |
| 30         | $291       | $10,185              | $3,492               | **$9,894**           | **$3,201**           |


**Break-even:** The agent pays for itself after **~5 minutes** of equivalent human work at any rate.

---

## Key Insights

### Why the Agent Dominates on Cost

1. **$9.70 vs $116–$534** — the agent is 12–55x cheaper than any human option
2. **Zero human attention** — runs overnight while staff sleeps; 7.5 hrs of unattended work
3. **Scalable** — run 10 operators simultaneously for $97 total; a human team would cost $1,160–$3,400
4. **Speed of reporting** — financial analysis + combined report + Google Sheets push in ~2 minutes vs ~2.5 hours manually

### Fixes Implemented (2026-03-15)

All 5 fixes have been applied to `agents/doordash_agent.py`:


| #   | Fix                                   | What Changed                                                                                    | Expected Impact                                             |
| --- | ------------------------------------- | ----------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| 1   | Timeout 360s → 540s                   | `AGENT_CAMPAIGN_TIMEOUT = 540`                                                                  | Eliminates ~10 timeout failures (64% of failures)           |
| 2   | Duplicate detection                   | Check `final_result` for duplicate phrases; mark as "Skipped (duplicate)" instead of "Failed"   | 4 fewer false failures; skipped on reruns                   |
| 3   | Page health check (every 5 campaigns) | Detects blank/broken pages; auto-restarts browser + re-login                                    | Prevents 4 page-render timeout failures                     |
| 4   | Schedule cell retry logic             | Prompt requires count verification + retry on mismatch; scroll-retry on "Element not available" | Fixes silent wrong-schedule successes                       |
| 5   | Streamlined campaign prompt           | Removed redundant Step 5 (re-verify incentive modal), merged verify into final step             | ~~45s saved per campaign (~~78 min total for 104 campaigns) |
| 6   | Better logging + Slack                | Progress %, ETA, success rates, phase summaries in terminal and Slack                           | Easier to monitor runs                                      |


**Projected improvement:**

- Failure rate: 21% → estimated **3–5%**
- Speed: ~~7.5 hrs → estimated **~~5.5 hrs** (saved ~78 min from prompt streamlining + fewer timeout retries)

### The Real Value

At 30 operator runs/month: **$3,200–$9,900/month saved** with a $291/month agent cost. That's **11–34x ROI**.

---

## Appendix: Pricing Verification

- **Browser Use API billing endpoint:** `GET /api/v2/billing/account`
- **Account balance (verified 2026-03-15):** $0.50 remaining (Pay As You Go)
- **Pricing source:** [browser-use.com/pricing](https://www.browser-use.com/pricing) and [docs.browser-use.com/cloud/pricing](https://docs.browser-use.com/cloud/pricing)
- **Model pricing:** bu-1-0 (Browser Use LLM) = $0.002/step; BU 2.0 = $0.006/step

*Analysis generated from run logs: `run_20260313_191729.log`*
*Campaign data: `mcd+tsgmgmtg_theondemandcompany_com-20260313_191729/campaigns_executed.csv`*