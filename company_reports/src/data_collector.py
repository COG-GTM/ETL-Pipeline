"""
Data Collector for UK Companies House
Scrapes company information from the Companies House website.
"""

import re
import time
from typing import Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import chromedriver_autoinstaller


class CompaniesHouseCollector:
    """Collects company data from UK Companies House website."""

    BASE_URL = "https://find-and-update.company-information.service.gov.uk"

    def __init__(self, headless: bool = True):
        """Initialize the collector with a headless Chrome browser."""
        self.headless = headless
        self.driver: Optional[webdriver.Chrome] = None

    def _setup_driver(self) -> webdriver.Chrome:
        """Set up and return a Chrome WebDriver instance."""
        chromedriver_autoinstaller.install()

        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        return webdriver.Chrome(options=chrome_options)

    def _wait_for_element(self, by: By, value: str, timeout: int = 10):
        """Wait for an element to be present and return it."""
        wait = WebDriverWait(self.driver, timeout)
        return wait.until(EC.presence_of_element_located((by, value)))

    def _safe_get_text(self, by: By, value: str, default: str = "N/A") -> str:
        """Safely get text from an element, returning default if not found."""
        try:
            element = self.driver.find_element(by, value)
            return element.text.strip() if element.text else default
        except NoSuchElementException:
            return default

    def _safe_get_texts(self, by: By, value: str) -> list:
        """Safely get text from multiple elements."""
        try:
            elements = self.driver.find_elements(by, value)
            return [el.text.strip() for el in elements if el.text.strip()]
        except NoSuchElementException:
            return []

    def search_company(self, company_name: str) -> Optional[str]:
        """Search for a company and return the URL of the first result."""
        search_url = f"{self.BASE_URL}/search/companies?q={company_name.replace(' ', '+')}"
        self.driver.get(search_url)
        time.sleep(2)

        try:
            results = self.driver.find_elements(By.CSS_SELECTOR, "ul.results-list li a")
            if results:
                for result in results:
                    result_text = result.text.upper()
                    if "TESCO" in result_text and "PLC" in result_text:
                        return result.get_attribute("href")
                return results[0].get_attribute("href")
        except NoSuchElementException:
            pass
        return None

    def get_company_profile(self, company_url: str) -> dict:
        """Get basic company profile information."""
        self.driver.get(company_url)
        time.sleep(2)

        profile = {
            "registered_name": "N/A",
            "company_number": "N/A",
            "registered_office_address": "N/A",
            "company_status": "N/A",
            "company_type": "N/A",
            "incorporation_date": "N/A",
            "country_of_origin": "United Kingdom",
            "sic_codes": []
        }

        try:
            name_elem = self.driver.find_element(By.CSS_SELECTOR, "h1.heading-xlarge, p.heading-xlarge")
            profile["registered_name"] = name_elem.text.strip()
        except NoSuchElementException:
            pass

        try:
            dl_elements = self.driver.find_elements(By.CSS_SELECTOR, "dl.column-two-thirds dt, dl dt")
            dd_elements = self.driver.find_elements(By.CSS_SELECTOR, "dl.column-two-thirds dd, dl dd")

            for dt, dd in zip(dl_elements, dd_elements):
                label = dt.text.strip().lower()
                value = dd.text.strip()

                if "company number" in label:
                    profile["company_number"] = value
                elif "registered office address" in label or "office address" in label:
                    profile["registered_office_address"] = value.replace("\n", ", ")
                elif "company status" in label:
                    profile["company_status"] = value
                elif "company type" in label:
                    profile["company_type"] = value
                elif "incorporated" in label:
                    profile["incorporation_date"] = value
        except NoSuchElementException:
            pass

        try:
            sic_section = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'SIC code')]/..")
            for section in sic_section:
                sic_text = section.text
                if "SIC code" in sic_text:
                    lines = sic_text.split("\n")
                    for line in lines:
                        if line.strip() and "SIC code" not in line:
                            profile["sic_codes"].append(line.strip())
        except NoSuchElementException:
            pass

        if not profile["sic_codes"]:
            try:
                sic_elements = self.driver.find_elements(By.XPATH, "//span[contains(@id, 'sic')]")
                for elem in sic_elements:
                    if elem.text.strip():
                        profile["sic_codes"].append(elem.text.strip())
            except NoSuchElementException:
                pass

        return profile

    def get_officers(self, company_url: str) -> list:
        """Get current officers (directors and secretary) information."""
        officers_url = f"{company_url}/officers"
        self.driver.get(officers_url)
        time.sleep(2)

        officers = []

        try:
            appointment_divs = self.driver.find_elements(By.CSS_SELECTOR, "div.appointment-1")

            for div in appointment_divs:
                try:
                    full_text = div.text
                    lines = [l.strip() for l in full_text.split("\n") if l.strip()]

                    name = ""
                    role = "Director"
                    appointed = "N/A"

                    name_link = div.find_elements(By.CSS_SELECTOR, "a")
                    if name_link:
                        name = name_link[0].text.strip()

                    for line in lines:
                        if "Director" in line:
                            role = "Director"
                        elif "Secretary" in line:
                            role = "Secretary"
                        if "Appointed on" in line:
                            appointed = line.replace("Appointed on", "").strip()
                        elif "Appointed" in line and appointed == "N/A":
                            appointed = line.replace("Appointed", "").strip()

                    if name and len(name) > 2:
                        officers.append({
                            "name": name,
                            "role": role,
                            "appointed_date": appointed
                        })
                except Exception:
                    continue

        except NoSuchElementException:
            pass

        if not officers:
            try:
                rows = self.driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
                for row in rows:
                    cells = row.find_elements(By.CSS_SELECTOR, "td")
                    if len(cells) >= 3:
                        name = cells[0].text.strip()
                        role = cells[1].text.strip() if len(cells) > 1 else "Director"
                        appointed = cells[2].text.strip() if len(cells) > 2 else "N/A"
                        if name and len(name) > 2:
                            officers.append({
                                "name": name,
                                "role": role,
                                "appointed_date": appointed
                            })
            except NoSuchElementException:
                pass

        return officers[:15]

    def get_filing_history(self, company_url: str) -> list:
        """Get recent filing history."""
        filings_url = f"{company_url}/filing-history"
        self.driver.get(filings_url)
        time.sleep(2)
        
        filings = []
        
        try:
            table = self.driver.find_element(By.CSS_SELECTOR, "table#fhTable, table")
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            
            for row in rows[:10]:
                cells = row.find_elements(By.CSS_SELECTOR, "td")
                if len(cells) >= 2:
                    date = cells[0].text.strip()
                    desc_cell = cells[1]
                    desc_links = desc_cell.find_elements(By.CSS_SELECTOR, "a, strong")
                    if desc_links:
                        description = desc_links[0].text.strip()
                    else:
                        description = desc_cell.text.strip()
                    if description and date:
                        filings.append({
                            "date": date,
                            "description": description[:150]
                        })
        except NoSuchElementException:
            pass
        
        return filings

    def get_charges(self, company_url: str) -> dict:
        """Get charges information."""
        charges_url = f"{company_url}/charges"
        self.driver.get(charges_url)
        time.sleep(2)

        charges_info = {
            "total_charges": 0,
            "outstanding": 0,
            "satisfied": 0,
            "details": []
        }

        try:
            page_text = self.driver.find_element(By.TAG_NAME, "body").text

            if "outstanding" in page_text.lower():
                outstanding_match = re.search(r"(\d+)\s*outstanding", page_text.lower())
                if outstanding_match:
                    charges_info["outstanding"] = int(outstanding_match.group(1))

            if "satisfied" in page_text.lower():
                satisfied_match = re.search(r"(\d+)\s*(?:fully\s+)?satisfied", page_text.lower())
                if satisfied_match:
                    charges_info["satisfied"] = int(satisfied_match.group(1))

            charges_info["total_charges"] = charges_info["outstanding"] + charges_info["satisfied"]

            charge_items = self.driver.find_elements(By.CSS_SELECTOR, "div.charge, li.charge-item, table tbody tr")
            for item in charge_items[:5]:
                text = item.text.strip()
                if text and len(text) > 10:
                    charges_info["details"].append(text[:300])

        except NoSuchElementException:
            pass

        return charges_info

    def collect_tesco_data(self) -> dict:
        """Collect all data for Tesco PLC."""
        self.driver = self._setup_driver()

        try:
            company_url = self.search_company("Tesco PLC")

            if not company_url:
                company_url = f"{self.BASE_URL}/company/00445790"

            print(f"Collecting data from: {company_url}")

            profile = self.get_company_profile(company_url)
            print(f"Profile collected: {profile['registered_name']}")

            officers = self.get_officers(company_url)
            print(f"Officers collected: {len(officers)} officers")

            filings = self.get_filing_history(company_url)
            print(f"Filings collected: {len(filings)} filings")

            charges = self.get_charges(company_url)
            print(f"Charges collected: {charges['total_charges']} total charges")

            return {
                "company_url": company_url,
                "profile": profile,
                "officers": officers,
                "filings": filings,
                "charges": charges,
                "collection_date": time.strftime("%Y-%m-%d %H:%M:%S")
            }

        finally:
            if self.driver:
                self.driver.quit()


def collect_company_data(company_name: str = "Tesco") -> dict:
    """Main function to collect company data."""
    collector = CompaniesHouseCollector(headless=True)

    if company_name.lower() == "tesco":
        return collector.collect_tesco_data()
    else:
        raise NotImplementedError(f"Collection for {company_name} not yet implemented")


if __name__ == "__main__":
    data = collect_company_data("Tesco")
    print("\n=== Collected Data ===")
    import json
    print(json.dumps(data, indent=2))
