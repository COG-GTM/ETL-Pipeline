"""
Git Workflow Automation
Handles branch creation, commits, and push operations.
"""

import os
import time
import subprocess
from typing import Optional


def run_git_command(command: list, cwd: str = None) -> tuple:
    """Run a git command and return (success, output)."""
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=60
        )
        return result.returncode == 0, result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return False, "Command timed out"
    except Exception as e:
        return False, str(e)


def create_branch(repo_path: str, branch_name: str = None) -> tuple:
    """Create and checkout a new branch."""
    if branch_name is None:
        timestamp = int(time.time())
        branch_name = f"devin/{timestamp}-tesco-onboarding-report"
    
    success, output = run_git_command(["git", "checkout", "-b", branch_name], cwd=repo_path)
    
    if not success and "already exists" in output:
        success, output = run_git_command(["git", "checkout", branch_name], cwd=repo_path)
    
    return success, branch_name, output


def add_file(repo_path: str, file_path: str) -> tuple:
    """Add a file to git staging."""
    return run_git_command(["git", "add", file_path], cwd=repo_path)


def commit_changes(repo_path: str, message: str) -> tuple:
    """Commit staged changes."""
    return run_git_command(["git", "commit", "-m", message], cwd=repo_path)


def push_branch(repo_path: str, branch_name: str, remote: str = "origin") -> tuple:
    """Push branch to remote with upstream tracking."""
    return run_git_command(
        ["git", "push", "--set-upstream", remote, branch_name],
        cwd=repo_path
    )


def get_current_branch(repo_path: str) -> Optional[str]:
    """Get the current branch name."""
    success, output = run_git_command(["git", "branch", "--show-current"], cwd=repo_path)
    if success:
        return output.strip()
    return None


def ensure_directory_exists(path: str) -> None:
    """Ensure a directory exists, creating it if necessary."""
    os.makedirs(path, exist_ok=True)


def execute_git_workflow(
    repo_path: str,
    pdf_path: str,
    branch_name: str = None,
    commit_message: str = "feat: add Tesco corporate onboarding report"
) -> dict:
    """Execute the complete git workflow."""
    results = {
        "success": False,
        "branch_name": None,
        "commit_message": commit_message,
        "errors": [],
        "steps": []
    }
    
    current_branch = get_current_branch(repo_path)
    if current_branch and current_branch.startswith("devin/"):
        results["branch_name"] = current_branch
        results["steps"].append(f"Using existing branch: {current_branch}")
    else:
        success, branch, output = create_branch(repo_path, branch_name)
        if not success:
            results["errors"].append(f"Failed to create branch: {output}")
            return results
        results["branch_name"] = branch
        results["steps"].append(f"Created branch: {branch}")
    
    if not os.path.exists(pdf_path):
        results["errors"].append(f"PDF file not found: {pdf_path}")
        return results
    
    relative_pdf_path = os.path.relpath(pdf_path, repo_path)
    success, output = add_file(repo_path, relative_pdf_path)
    if not success:
        results["errors"].append(f"Failed to add PDF: {output}")
        return results
    results["steps"].append(f"Added file: {relative_pdf_path}")
    
    success, output = commit_changes(repo_path, commit_message)
    if not success:
        if "nothing to commit" in output.lower():
            results["steps"].append("No changes to commit (file already committed)")
        else:
            results["errors"].append(f"Failed to commit: {output}")
            return results
    else:
        results["steps"].append(f"Committed with message: {commit_message}")
    
    results["success"] = True
    return results


if __name__ == "__main__":
    import sys
    
    repo_path = sys.argv[1] if len(sys.argv) > 1 else "."
    pdf_path = sys.argv[2] if len(sys.argv) > 2 else "./tesco_company_onboarding_report.pdf"
    
    result = execute_git_workflow(repo_path, pdf_path)
    
    print("\n=== Git Workflow Results ===")
    print(f"Success: {result['success']}")
    print(f"Branch: {result['branch_name']}")
    print("\nSteps:")
    for step in result["steps"]:
        print(f"  - {step}")
    if result["errors"]:
        print("\nErrors:")
        for error in result["errors"]:
            print(f"  - {error}")
