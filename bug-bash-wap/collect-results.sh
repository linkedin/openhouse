#!/bin/bash
# Bug Bash Results Collection Script

echo "========================================="
echo "Bug Bash Results Summary"
echo "========================================="
echo ""

# Count completion
total_tests=$(ls results/*.md 2>/dev/null | wc -l | tr -d ' ')
completed=$(grep -l "✅ PASS\|❌ FAIL\|⚠️ PARTIAL" results/*.md | wc -l | tr -d ' ')
in_progress=$(grep -l "🔄 IN PROGRESS" results/*.md | wc -l | tr -d ' ')
not_started=$(grep -l "🔲 NOT STARTED" results/*.md | wc -l | tr -d ' ')

echo "Completion Status:"
echo "  Total Tests:    $total_tests"
echo "  Completed:      $completed"
echo "  In Progress:    $in_progress"
echo "  Not Started:    $not_started"
echo ""

# Results breakdown
passed=$(grep -l "✅ PASS" results/*.md | wc -l | tr -d ' ')
failed=$(grep -l "❌ FAIL" results/*.md | wc -l | tr -d ' ')
partial=$(grep -l "⚠️ PARTIAL" results/*.md | wc -l | tr -d ' ')

echo "Test Results:"
echo "  ✅ Passed:      $passed"
echo "  ❌ Failed:      $failed"
echo "  ⚠️  Partial:     $partial"
echo ""

echo "========================================="
echo "Detailed Results by Test"
echo "========================================="
echo ""

for file in results/*.md; do
  test_name=$(basename "$file" .md)
  status=$(grep "^\*\*Status:\*\*" "$file" | sed 's/.*Status:\*\* //')
  assignee=$(grep "^\*\*Assignee:\*\*" "$file" | sed 's/.*Assignee:\*\* //')
  
  # Check for issues (check if Bug found checkbox is marked)
  if grep -q "\- \[x\] Bug found:" "$file" 2>/dev/null || grep -q "Bug found:.*\[x\]" "$file" 2>/dev/null; then
    has_bugs="🐛"
  else
    has_bugs=""
  fi
  
  echo "[$test_name] $assignee - $status $has_bugs"
done

echo ""
echo "========================================="
echo "Issues Found"
echo "========================================="
echo ""

# Find all bug reports
bug_count=0
for file in results/*.md; do
  # Check if bug checkbox is actually marked [x]
  if grep "\- \[x\] Bug found:" "$file" >/dev/null 2>&1; then
    test_name=$(basename "$file" .md)
    assignee=$(grep "^\*\*Assignee:\*\*" "$file" | sed 's/.*Assignee:\*\* //')
    # Get the bug description (line after "Bug found:")
    bug_desc=$(grep -A 1 "\- \[x\] Bug found:" "$file" | tail -1 | sed 's/^[[:space:]]*//')
    if [ -n "$bug_desc" ]; then
      echo "🐛 [$test_name] by $assignee"
      echo "   $bug_desc"
      echo ""
      bug_count=$((bug_count + 1))
    fi
  fi
done

if [ $bug_count -eq 0 ]; then
  echo "No bugs reported yet."
fi

echo ""
echo "========================================="
echo "Collection complete!"
echo "========================================="

