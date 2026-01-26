"""
Main Orchestration Script
Coordinates data collection, PDF generation, and Git workflow.
"""

import os
import sys
import argparse
from datetime import datetime

from data_collector import collect_company_data
from pdf_generator import generate_pdf
from git_workflow import execute_git_workflow


def get_output_path(company_name: str, output_dir: str = None) -> str:
    """Generate the output path for the PDF report."""
    if output_dir is None:
        output_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    filename = f"{company_name.lower().replace(' ', '_')}_company_onboarding_report.pdf"
    return os.path.join(output_dir, filename)


def run_report_generation(
    company_name: str = "Tesco",
    output_dir: str = None,
    skip_git: bool = False,
    verbose: bool = True
) -> dict:
    """Run the complete report generation workflow."""
    results = {
        "success": False,
        "company_name": company_name,
        "pdf_path": None,
        "git_results": None,
        "errors": [],
        "steps": []
    }
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"Corporate Onboarding Report Generator")
        print(f"Company: {company_name}")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")
    
    if verbose:
        print("[1/3] Collecting company data from Companies House...")
    
    try:
        company_data = collect_company_data(company_name)
        results["steps"].append("Data collection completed")
        
        if verbose:
            profile = company_data.get("profile", {})
            print(f"      Company: {profile.get('registered_name', 'N/A')}")
            print(f"      Status: {profile.get('company_status', 'N/A')}")
            print(f"      Officers: {len(company_data.get('officers', []))}")
            print(f"      Filings: {len(company_data.get('filings', []))}")
            print(f"      Charges: {company_data.get('charges', {}).get('total_charges', 0)}")
            print()
    except Exception as e:
        error_msg = f"Data collection failed: {str(e)}"
        results["errors"].append(error_msg)
        if verbose:
            print(f"      ERROR: {error_msg}")
        return results
    
    if verbose:
        print("[2/3] Generating PDF report...")
    
    try:
        pdf_path = get_output_path(company_name, output_dir)
        generate_pdf(company_data, pdf_path)
        results["pdf_path"] = pdf_path
        results["steps"].append(f"PDF generated: {pdf_path}")
        
        if verbose:
            file_size = os.path.getsize(pdf_path) / 1024
            print(f"      Output: {pdf_path}")
            print(f"      Size: {file_size:.1f} KB")
            print()
    except Exception as e:
        error_msg = f"PDF generation failed: {str(e)}"
        results["errors"].append(error_msg)
        if verbose:
            print(f"      ERROR: {error_msg}")
        return results
    
    if not skip_git:
        if verbose:
            print("[3/3] Executing Git workflow...")
        
        try:
            repo_path = output_dir or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            git_results = execute_git_workflow(repo_path, pdf_path)
            results["git_results"] = git_results
            
            if git_results["success"]:
                results["steps"].append(f"Git workflow completed on branch: {git_results['branch_name']}")
                if verbose:
                    print(f"      Branch: {git_results['branch_name']}")
                    for step in git_results["steps"]:
                        print(f"      - {step}")
            else:
                for error in git_results.get("errors", []):
                    results["errors"].append(f"Git: {error}")
                if verbose:
                    print(f"      WARNING: Git workflow had issues")
                    for error in git_results.get("errors", []):
                        print(f"      - {error}")
        except Exception as e:
            error_msg = f"Git workflow failed: {str(e)}"
            results["errors"].append(error_msg)
            if verbose:
                print(f"      ERROR: {error_msg}")
    else:
        if verbose:
            print("[3/3] Skipping Git workflow (--skip-git flag)")
        results["steps"].append("Git workflow skipped")
    
    results["success"] = len(results["errors"]) == 0
    
    if verbose:
        print(f"\n{'='*60}")
        if results["success"]:
            print("Report generation completed successfully!")
        else:
            print("Report generation completed with errors:")
            for error in results["errors"]:
                print(f"  - {error}")
        print(f"{'='*60}\n")
    
    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate corporate onboarding reports from UK Companies House data"
    )
    parser.add_argument(
        "--company",
        default="Tesco",
        help="Company name to generate report for (default: Tesco)"
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for the PDF report"
    )
    parser.add_argument(
        "--skip-git",
        action="store_true",
        help="Skip Git workflow (branch creation, commit, push)"
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress verbose output"
    )
    
    args = parser.parse_args()
    
    results = run_report_generation(
        company_name=args.company,
        output_dir=args.output_dir,
        skip_git=args.skip_git,
        verbose=not args.quiet
    )
    
    sys.exit(0 if results["success"] else 1)


if __name__ == "__main__":
    main()
