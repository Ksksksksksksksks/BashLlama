import json
import re
from pathlib import Path

PROCESSED_DIR = Path("../processed")

# Allowed characters: letters (including Cyrillic/Bashkir), digits, basic punctuation
# Pattern to match allowed characters
ALLOWED_PATTERN = re.compile(r'^[\w\s\-–—:;,.!?\'"«»(){}\[\]<>*+/=№%$@&~`|]+$')

# Characters that should NOT appear
FORBIDDEN_CHARS = ['\t', '\r', '\\', '/', '\x0b', '\x0c']

def check_text(text, filename, line_num):
    """Check if text contains only allowed characters"""
    issues = []
    
    # Check for forbidden characters
    for char in FORBIDDEN_CHARS:
        if char in text:
            issues.append(f"Found forbidden char '{repr(char)}'")
    
    # Check for multiple consecutive newlines (should be normalized)
    if '\n\n\n' in text:
        issues.append("Found triple newlines")
    
    # Check for weird unicode characters
    for char in text:
        code = ord(char)
        # Control characters except \n and \t (already checked above)
        if code < 32 and code not in [10, 13]:
            issues.append(f"Found control char code {code}")
        # Private use area
        if 0xE000 <= code <= 0xF8FF:
            issues.append(f"Found private use char code {code}")
    
    return issues


def verify_file(filename):
    filepath = PROCESSED_DIR / filename
    print(f"\nChecking {filename}...")
    
    total_lines = 0
    issues_found = 0
    
    with open(filepath, encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            total_lines += 1
            try:
                item = json.loads(line)
                text = item.get("text", "")
                
                issues = check_text(text, filename, line_num)
                if issues:
                    issues_found += 1
                    print(f"  Line {line_num}: {', '.join(issues)}")
                    print(f"    Text preview: {text[:100]}...")
                    
                    if issues_found >= 5:  # Show first 5 issues per file
                        print(f"  ... (stopping after {issues_found} issues)")
                        break
                        
            except Exception as e:
                print(f"  ERROR parsing line {line_num}: {e}")
                issues_found += 1
    
    if issues_found == 0:
        print(f"  [OK] ({total_lines} lines)")
    else:
        print(f"  [ERROR] Found {issues_found} issues")
    
    return issues_found


if __name__ == "__main__":
    jsonl_files = list(PROCESSED_DIR.glob("*_clean.jsonl"))
    
    total_issues = 0
    for file in jsonl_files:
        issues = verify_file(file.name)
        total_issues += issues
    
    print(f"\n{'='*50}")
    if total_issues == 0:
        print("[OK] All files are clean!")
    else:
        print(f"[ERROR] Total issues found: {total_issues}")
