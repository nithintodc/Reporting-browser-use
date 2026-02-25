"""
MarketingCampaignAgent: sets up a new DoorDash campaign via the merchant portal.

Flow (assumes already on merchant portal, e.g. after DoorDashAgent login):
1. Side panel: Marketing → Campaigns
2. Create campaign → Select "Discount for all customers"
3. Customise your campaign → Edit Customer incentive: % off, min subtotal, max discount (lowest)
4. Target audience: New customers
5. Scheduling: Set a custom schedule → Weekdays/Weekends off → Select Breakfast + Monday → Save
6. Campaign name: TODC-test (or provided name)
7. Create promotion

Uses flexible selectors (role, text) for resilience. If the UI differs, screenshots can help refine.
"""

import asyncio
import logging
from typing import Optional

from playwright.async_api import Page

logger = logging.getLogger(__name__)
# To see debug statements (modal scope, button clicks, modal close): set level to DEBUG, e.g.:
# logging.getLogger("agents.marketing_campaign_agent").setLevel(logging.DEBUG)

STEP_DELAY_SEC = 5
CLICK_TIMEOUT_MS = 15_000
# Longer timeout for full page/context transitions (e.g. after Select on campaign type)
PAGE_TRANSITION_TIMEOUT_MS = 30_000


class MarketingCampaignAgent:
    """Creates a new DoorDash marketing campaign with the specified incentive and schedule."""

    def __init__(self, page: Page) -> None:
        self.page = page

    async def run(
        self,
        discount_pct: int = 20,
        min_subtotal: float = 20.0,
        campaign_name: str = "TODC-test",
    ) -> bool:
        """
        Run the full campaign setup flow.
        discount_pct: e.g. 20 for 20% off (from campaign recommendations).
        min_subtotal: minimum order (from campaign recommendations, e.g. B or C).
        campaign_name: e.g. TODC-test.
        Returns True if flow completed without critical failure.
        """
        try:
            await self._go_to_marketing_campaigns()
            await self._click_create_campaign()
            await self._select_discount_for_all_customers()
            await self._customise_customer_incentive(discount_pct, min_subtotal)
            await self._set_target_audience_new_customers()
            await self._set_schedule_breakfast_monday()
            await self._set_campaign_name_and_create(campaign_name)
            return True
        except Exception as e:
            logger.warning("MarketingCampaignAgent: %s", e)
            return False

    async def _go_to_marketing_campaigns(self) -> None:
        """Side panel: click Marketing, then Campaigns."""
        logger.info("MarketingCampaignAgent: Opening Marketing")
        marketing = self.page.get_by_role("link", name="Marketing").or_(
            self.page.get_by_text("Marketing", exact=False)
        ).first
        await marketing.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await marketing.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)

        logger.info("MarketingCampaignAgent: Opening Campaigns")
        campaigns = self.page.get_by_role("link", name="Campaigns").or_(
            self.page.get_by_text("Campaigns", exact=False)
        ).first
        await campaigns.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await campaigns.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)

    async def _click_create_campaign(self) -> None:
        """Click Create campaign button."""
        logger.info("MarketingCampaignAgent: Click Create campaign")
        btn = self.page.get_by_role("button", name="Create campaign").or_(
            self.page.get_by_text("Create campaign", exact=False)
        ).first
        await btn.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await btn.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)

    async def _select_discount_for_all_customers(self) -> None:
        """Click Select on the 'Discount for all customers' card, then wait for campaign setup view."""
        logger.info("MarketingCampaignAgent: Select Discount for all customers")
        card = self.page.get_by_text("Discount for all customers", exact=False).first
        await card.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        select_btn = self.page.get_by_role("button", name="Select").or_(
            self.page.get_by_text("Select", exact=False)
        ).first
        await select_btn.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await select_btn.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)
        # Wait for the campaign setup view (Customise your campaign) to appear; transition can be slow
        customise_heading = self.page.get_by_text("Customise your campaign", exact=False).or_(
            self.page.get_by_text("Customize your campaign", exact=False)
        ).first
        await customise_heading.wait_for(state="visible", timeout=PAGE_TRANSITION_TIMEOUT_MS)
        logger.debug("MarketingCampaignAgent: Campaign setup view visible")
        await asyncio.sleep(STEP_DELAY_SEC)

    async def _customise_customer_incentive(self, discount_pct: int, min_subtotal: float) -> None:
        """Customise campaign → Edit Customer incentive (pencil) → popup: % Off, min basket, lowest max discount → Save."""
        logger.info("MarketingCampaignAgent: Customise your campaign")
        customise = self.page.get_by_text("Customise your campaign", exact=False).or_(
            self.page.get_by_text("Customize your campaign", exact=False)
        ).first
        await customise.wait_for(state="visible", timeout=PAGE_TRANSITION_TIMEOUT_MS)
        await customise.click()
        await asyncio.sleep(STEP_DELAY_SEC)

        logger.info("MarketingCampaignAgent: Edit Customer incentive (pencil icon)")
        customer_incentive_label = self.page.get_by_text("Customer incentive", exact=False).first
        await customer_incentive_label.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        section = customer_incentive_label.locator("xpath=ancestor::*[.//button][1]")
        edit_btn = section.locator("button:has(svg), button[aria-label*='Edit'], button[title*='Edit']").first
        if await edit_btn.count() == 0:
            edit_btn = section.locator("button").first
        await edit_btn.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await edit_btn.click()
        await asyncio.sleep(STEP_DELAY_SEC)

        # Popup "Set customer incentive": scope all actions to the modal so we don't click the page behind
        incentive_title = self.page.get_by_text("Set customer incentive", exact=False).first
        await incentive_title.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        logger.debug("MarketingCampaignAgent: Incentive modal visible")
        await asyncio.sleep(STEP_DELAY_SEC)

        # Scope to dialog (modal) containing the title; fallback to page if no role=dialog
        modal = self.page.get_by_role("dialog").filter(has=incentive_title).first
        if await modal.count() == 0:
            modal = incentive_title.locator("xpath=ancestor::*[contains(@role,'dialog') or contains(@class,'modal') or contains(@class,'popup')][1]").first
        if await modal.count() == 0:
            modal = self.page  # fallback: use page (might click wrong element if multiple modals)
        else:
            logger.debug("MarketingCampaignAgent: Using modal scope for incentive popup")

        logger.info("MarketingCampaignAgent: Select %% Off (percentage discount)")
        pct_off = modal.get_by_text("% Off", exact=False).or_(modal.get_by_role("button", name="% Off")).first
        if await pct_off.count() > 0:
            await pct_off.click()
            logger.debug("MarketingCampaignAgent: Clicked %% Off")
            await asyncio.sleep(STEP_DELAY_SEC)
        else:
            logger.debug("MarketingCampaignAgent: %% Off control not found")

        # Select discount %: e.g. "20%" or "Custom offer" and fill
        pct_opt = modal.get_by_text(f"{discount_pct}%", exact=False).first
        if await pct_opt.count() > 0:
            await pct_opt.click()
            logger.debug("MarketingCampaignAgent: Selected %s%% option", discount_pct)
            await asyncio.sleep(STEP_DELAY_SEC)
        else:
            custom_offer = modal.get_by_text("Custom offer", exact=False).first
            if await custom_offer.count() > 0:
                await custom_offer.click()
                await asyncio.sleep(STEP_DELAY_SEC)
            pct_input = modal.locator('input[placeholder*="%"], input[name*="percent"], input[type="number"]').first
            if await pct_input.count() > 0:
                await pct_input.fill(str(discount_pct))
                logger.debug("MarketingCampaignAgent: Filled custom %% = %s", discount_pct)
                await asyncio.sleep(STEP_DELAY_SEC)

        logger.info("MarketingCampaignAgent: Set minimum subtotal (min basket)")
        min_section = modal.get_by_text("Minimum subtotal", exact=False).first
        if await min_section.count() > 0:
            container = min_section.locator("xpath=ancestor::*[.//button][1]")
            custom_btn = container.get_by_text("Custom", exact=False).first
            if await custom_btn.count() > 0:
                await custom_btn.click()
                await asyncio.sleep(STEP_DELAY_SEC)
                min_input = modal.locator('input[placeholder*="Minimum"], input[name*="min"], input[type="number"]').first
                if await min_input.count() > 0:
                    await min_input.fill(str(int(min_subtotal) if min_subtotal == int(min_subtotal) else min_subtotal))
                    logger.debug("MarketingCampaignAgent: Min subtotal = %s", min_subtotal)
                    await asyncio.sleep(STEP_DELAY_SEC)
            else:
                preset = container.get_by_text(f"${int(min_subtotal)}", exact=False).first
                if await preset.count() > 0:
                    await preset.click()
                    logger.debug("MarketingCampaignAgent: Clicked preset $%s", int(min_subtotal))
                    await asyncio.sleep(STEP_DELAY_SEC)
        else:
            logger.debug("MarketingCampaignAgent: Minimum subtotal section not found")

        logger.info("MarketingCampaignAgent: Set lowest maximum discount ($7)")
        max_section = modal.get_by_text("Maximum discount amount", exact=False).or_(
            modal.get_by_text("Maximum discount", exact=False)
        ).first
        if await max_section.count() > 0:
            # Within this section, click the button that has exactly "$7" (lowest option)
            container = max_section.locator("xpath=ancestor::*[.//button][1]")
            lowest_btn = container.get_by_role("button", name="$7").or_(container.get_by_text("$7", exact=True)).first
            if await lowest_btn.count() > 0:
                await lowest_btn.click()
                logger.debug("MarketingCampaignAgent: Clicked $7 max discount")
                await asyncio.sleep(STEP_DELAY_SEC)
            else:
                first_btn = container.locator("button").first
                if await first_btn.count() > 0:
                    await first_btn.click()
                    logger.debug("MarketingCampaignAgent: Clicked first max-discount button (fallback)")
                    await asyncio.sleep(STEP_DELAY_SEC)
                else:
                    logger.debug("MarketingCampaignAgent: No $7 or first button found in max discount section")
        else:
            logger.debug("MarketingCampaignAgent: Maximum discount section not found")

        logger.info("MarketingCampaignAgent: Save incentive")
        save_btn = modal.get_by_role("button", name="Save").or_(modal.get_by_text("Save", exact=False)).first
        await save_btn.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await save_btn.click()
        logger.debug("MarketingCampaignAgent: Save incentive clicked, waiting for modal to close")
        # Wait for incentive modal to close before interacting with the page
        await incentive_title.wait_for(state="hidden", timeout=CLICK_TIMEOUT_MS)
        logger.debug("MarketingCampaignAgent: Incentive modal closed")
        await asyncio.sleep(STEP_DELAY_SEC)

    async def _set_target_audience_new_customers(self) -> None:
        """Open Target audience via pencil → popup "Set target audience" → New customers → Save."""
        logger.info("MarketingCampaignAgent: Open Target audience (pencil icon)")
        audience_label = self.page.get_by_text("Target audience", exact=False).first
        await audience_label.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        section = audience_label.locator("xpath=ancestor::*[.//button][1]")
        edit_btn = section.locator("button:has(svg), button[aria-label*='Edit'], button[title*='Edit']").first
        if await edit_btn.count() == 0:
            edit_btn = section.locator("button").first
        await edit_btn.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await edit_btn.click()
        logger.debug("MarketingCampaignAgent: Clicked Target audience edit (pencil)")
        await asyncio.sleep(STEP_DELAY_SEC)

        audience_title = self.page.get_by_text("Set target audience", exact=False).first
        await audience_title.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        logger.debug("MarketingCampaignAgent: Target audience modal visible")
        await asyncio.sleep(STEP_DELAY_SEC)

        modal = self.page.get_by_role("dialog").filter(has=audience_title).first
        if await modal.count() == 0:
            modal = self.page
        else:
            logger.debug("MarketingCampaignAgent: Using modal scope for target audience popup")

        new_customers = modal.get_by_text("New customers", exact=False).or_(
            modal.get_by_role("radio", name="New customers")
        ).first
        await new_customers.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await new_customers.click()
        logger.debug("MarketingCampaignAgent: Selected New customers")
        await asyncio.sleep(STEP_DELAY_SEC)

        save_btn = modal.get_by_role("button", name="Save").or_(modal.get_by_text("Save", exact=False)).first
        await save_btn.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await save_btn.click()
        logger.debug("MarketingCampaignAgent: Save target audience clicked, waiting for modal to close")
        await audience_title.wait_for(state="hidden", timeout=CLICK_TIMEOUT_MS)
        logger.debug("MarketingCampaignAgent: Target audience modal closed")
        await asyncio.sleep(STEP_DELAY_SEC)

    async def _set_schedule_breakfast_monday(self) -> None:
        """Find Scheduling → click pencil → popup Set a schedule → Set a custom schedule → Next → Set custom schedule: Weekdays, Weekends, Monday + Breakfast → Save."""
        logger.info("MarketingCampaignAgent: Open Scheduling (pencil icon)")
        scheduling_label = self.page.get_by_text("Scheduling", exact=False).first
        await scheduling_label.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        section = scheduling_label.locator("xpath=ancestor::*[.//button][1]")
        edit_btn = section.locator("button:has(svg), button[aria-label*='Edit'], button[title*='Edit']").first
        if await edit_btn.count() == 0:
            edit_btn = section.locator("button").first
        await edit_btn.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await edit_btn.click()
        logger.debug("MarketingCampaignAgent: Clicked Scheduling edit (pencil)")
        await asyncio.sleep(2)  # allow modal to open/animate

        # Wait for "Set a schedule" popup: dialog containing title, or title text (modal may not use role="dialog")
        try:
            await self.page.get_by_role("dialog").filter(has_text="Set a schedule").first.wait_for(
                state="visible", timeout=CLICK_TIMEOUT_MS
            )
            logger.debug("MarketingCampaignAgent: Set a schedule dialog visible")
        except Exception:
            await self.page.get_by_text("Set a schedule", exact=False).first.wait_for(
                state="visible", timeout=CLICK_TIMEOUT_MS
            )
            logger.debug("MarketingCampaignAgent: Set a schedule title visible")
        await asyncio.sleep(STEP_DELAY_SEC)

        custom_schedule = self.page.get_by_text("Set a custom schedule", exact=False).or_(
            self.page.get_by_role("radio", name="Set a custom schedule")
        ).first
        await custom_schedule.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await custom_schedule.click()
        await asyncio.sleep(STEP_DELAY_SEC)

        next_btn = self.page.get_by_role("button", name="Next").first
        await next_btn.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await next_btn.click()
        await asyncio.sleep(STEP_DELAY_SEC)

        await self.page.get_by_text("Set custom schedule", exact=False).first.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await asyncio.sleep(STEP_DELAY_SEC)

        logger.info("MarketingCampaignAgent: Click Weekdays and Weekends tabs")
        weekdays_tab = self.page.get_by_text("Weekdays", exact=False).first
        if await weekdays_tab.count() > 0:
            await weekdays_tab.click()
            await asyncio.sleep(STEP_DELAY_SEC)
        weekends_tab = self.page.get_by_text("Weekends", exact=False).first
        if await weekends_tab.count() > 0:
            await weekends_tab.click()
            await asyncio.sleep(STEP_DELAY_SEC)

        logger.info("MarketingCampaignAgent: Select Monday and Breakfast")
        monday = self.page.get_by_text("Mon", exact=False).or_(self.page.get_by_text("Monday", exact=False)).first
        await monday.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await monday.click()
        await asyncio.sleep(STEP_DELAY_SEC)
        breakfast = self.page.get_by_text("Breakfast", exact=False).first
        await breakfast.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await breakfast.click()
        await asyncio.sleep(STEP_DELAY_SEC)

        # Save inside "Set custom schedule" modal so we don't click a Save on the page behind
        custom_schedule_title = self.page.get_by_text("Set custom schedule", exact=False).first
        schedule_modal = self.page.get_by_role("dialog").filter(has_text="Set custom schedule").first
        if await schedule_modal.count() > 0:
            save_btn = schedule_modal.get_by_role("button", name="Save").or_(
                schedule_modal.get_by_text("Save", exact=False)
            ).first
        else:
            save_btn = self.page.get_by_role("button", name="Save").or_(
                self.page.get_by_text("Save", exact=False)
            ).first
        await save_btn.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await save_btn.click()
        logger.debug("MarketingCampaignAgent: Save custom schedule clicked, waiting for modal to close")
        await custom_schedule_title.wait_for(state="hidden", timeout=CLICK_TIMEOUT_MS)
        logger.debug("MarketingCampaignAgent: Set custom schedule modal closed")
        await asyncio.sleep(STEP_DELAY_SEC)

    async def _set_campaign_name_and_create(self, campaign_name: str) -> None:
        """Click pencil on Campaign name → Edit campaign name popup → enter name (e.g. TODC-test) → Save → Create promotion."""
        logger.info("MarketingCampaignAgent: Open Edit campaign name (pencil icon)")
        campaign_name_label = self.page.get_by_text("Campaign name", exact=False).first
        await campaign_name_label.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        section = campaign_name_label.locator("xpath=ancestor::*[.//button][1]")
        edit_btn = section.locator("button:has(svg), button[aria-label*='Edit'], button[title*='Edit']").first
        if await edit_btn.count() == 0:
            edit_btn = section.locator("button").first
        await edit_btn.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await edit_btn.click()
        logger.debug("MarketingCampaignAgent: Clicked Campaign name edit (pencil)")
        await asyncio.sleep(STEP_DELAY_SEC)

        edit_title = self.page.get_by_text("Edit campaign name", exact=False).first
        await edit_title.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        logger.debug("MarketingCampaignAgent: Edit campaign name popup visible")
        await asyncio.sleep(STEP_DELAY_SEC)

        # Scope to the Edit campaign name dialog; the only text input in it is the campaign name field
        name_modal = self.page.get_by_role("dialog").filter(has_text="Edit campaign name").first
        if await name_modal.count() == 0:
            name_modal = self.page
        else:
            logger.debug("MarketingCampaignAgent: Using modal scope for Edit campaign name popup")

        # Single input in this popup is the campaign name (no label association needed)
        name_input = name_modal.locator("input").first
        await name_input.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await name_input.click()
        await name_input.clear()
        await name_input.fill(campaign_name)
        logger.debug("MarketingCampaignAgent: Filled campaign name = %s", campaign_name)
        await asyncio.sleep(STEP_DELAY_SEC)

        save_btn = name_modal.get_by_role("button", name="Save").or_(name_modal.get_by_text("Save", exact=False)).first
        await save_btn.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await save_btn.click()
        logger.debug("MarketingCampaignAgent: Campaign name saved, waiting for modal to close")
        await edit_title.wait_for(state="hidden", timeout=CLICK_TIMEOUT_MS)
        logger.debug("MarketingCampaignAgent: Edit campaign name modal closed")
        await asyncio.sleep(STEP_DELAY_SEC)

        logger.info("MarketingCampaignAgent: Create promotion")
        create_btn = self.page.get_by_role("button", name="Create promotion").or_(
            self.page.get_by_text("Create promotion", exact=False)
        ).first
        await create_btn.wait_for(state="visible", timeout=CLICK_TIMEOUT_MS)
        await create_btn.scroll_into_view_if_needed()
        await asyncio.sleep(1)  # let layout settle after scroll
        await create_btn.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)
        logger.info("MarketingCampaignAgent: Campaign creation submitted")
