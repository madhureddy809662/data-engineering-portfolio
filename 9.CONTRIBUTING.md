# Contributing Guidelines

Thank you for your interest in contributing to this Data Engineering portfolio! Whether it's bug fixes, improvements, or new examples, all contributions are welcome.

---

## 🚀 How to Contribute

### Reporting Issues

Found a bug or have a suggestion? [Open an issue](https://github.com/madhureddy809662/Data-Enginnering/issues) with:
- Clear description of the problem
- Steps to reproduce (if applicable)
- Expected vs. actual behavior
- Your environment (OS, Python version, Spark version)

### Submitting Pull Requests

1. **Fork** the repository
2. **Create a branch** for your feature: `git checkout -b feature/your-feature-name`
3. **Make changes** following the code style guidelines below
4. **Test thoroughly** - ensure existing tests pass
5. **Commit** with clear messages: `git commit -m "Add feature: description"`
6. **Push** to your fork: `git push origin feature/your-feature-name`
7. **Open a PR** with a description of your changes

### Code Style Guidelines

**Python:**
```python
# Follow PEP 8
# - Line length: 88 characters (Black formatter)
# - Docstrings: Google-style ("""Description with examples""")
# - Type hints: Use for public functions

def process_data(df: DataFrame, partition_col: str) -> DataFrame:
    """Process input dataframe with given partition column.
    
    Args:
        df: Input Spark DataFrame
        partition_col: Column to partition by
        
    Returns:
        Processed DataFrame partitioned by partition_col
        
    Example:
        >>> processed = process_data(df, "date")
    """
    return df.repartition(partition_col)
```

**SQL:**
```sql
-- Use consistent formatting
-- Keywords: UPPERCASE
-- Identifiers: lowercase_with_underscores
SELECT 
    customer_id,
    SUM(amount) as total_spent,
    COUNT(*) as transaction_count
FROM fact_sales
WHERE date_key >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY customer_id
ORDER BY total_spent DESC
```

### Commit Message Format

```
[TYPE] Brief description (50 chars max)

Longer explanation if needed. Explain what and why, not how.
Keep to 72 characters per line.

Fixes #123 (if fixing an issue)
```

**Types:**
- `[FEAT]` - New feature
- `[FIX]` - Bug fix
- `[DOCS]` - Documentation
- `[TEST]` - Tests
- `[REFACTOR]` - Code refactoring
- `[PERF]` - Performance improvement
- `[CI]` - CI/CD configuration

---

## 📋 Types of Contributions Welcome

### 1. **Bug Fixes**
- Fix errors in existing code
- Improve performance
- Resolve documentation issues

### 2. **New Examples**
- Add code examples for existing projects
- Create new optimization techniques
- Add interview question solutions

### 3. **Documentation**
- Improve README clarity
- Add troubleshooting guides
- Create setup guides for different platforms

### 4. **Tests**
- Add unit tests
- Add integration tests
- Improve test coverage

### 5. **Infrastructure**
- Improve Terraform modules
- Add GitHub Actions workflows
- Enhance deployment process

---

## 🧪 Testing Requirements

Before submitting a PR:

```bash
# Run tests
pytest tests/ -v

# Check code style
black --check src/
flake8 src/ --max-line-length=88

# Type checking (optional but appreciated)
mypy src/

# Generate coverage report
pytest tests/ --cov=src/ --cov-report=html
```

---

## 📖 Documentation Standards

- **README:** Clear project overview, quick start, key features
- **Docstrings:** Explain what, args, returns, examples
- **Comments:** Explain why, not what
- **Diagrams:** ASCII art for architecture, links to high-res PNG

Example docstring:
```python
def calculate_metrics(df: DataFrame) -> Dict[str, float]:
    """Calculate key performance metrics from events dataframe.
    
    Computes:
    - Average event value
    - Event count
    - Unique user count
    
    Args:
        df: Events dataframe with columns [event_id, user_id, value]
        
    Returns:
        Dictionary with keys: avg_value, count, unique_users
        
    Raises:
        ValueError: If required columns missing from dataframe
        
    Example:
        >>> metrics = calculate_metrics(events_df)
        >>> print(f"Avg value: ${metrics['avg_value']:.2f}")
        Avg value: $125.50
    """
```

---

## 🔍 Code Review Process

All PRs will be reviewed for:
- ✅ Correctness (does it work?)
- ✅ Style (follows guidelines?)
- ✅ Tests (adequately tested?)
- ✅ Documentation (clear & complete?)
- ✅ Performance (efficient?)
- ✅ Maintainability (easy to understand?)

---

## 💡 Project Scope

**In Scope:**
- Data engineering patterns & best practices
- Performance optimization techniques
- Production-ready code examples
- System design solutions
- Interview preparation materials

**Out of Scope:**
- Web development tutorials
- Non-data-engineering topics
- Marketing/promotional content
- Solutions to online coding challenges

---

## 🤝 Community Guidelines

This project follows the **Contributor Covenant Code of Conduct**:

- Be respectful and inclusive
- Constructive feedback only
- No harassment, discrimination, or trolling
- Assume good intent

---

## 📝 Areas for Contribution

### High Priority
- [ ] Add Kafka integration examples
- [ ] Create Unity Catalog setup guide
- [ ] Add streaming deduplication patterns
- [ ] Improve performance benchmarks

### Medium Priority
- [ ] Add more interview questions
- [ ] Create video walkthrough guides
- [ ] Add Azure Synapse examples
- [ ] Enhance error handling

### Nice to Have
- [ ] Docker compose for local setup
- [ ] Kubernetes deployment examples
- [ ] Cloud cost calculator
- [ ] Performance tuning checklist

---

## 📞 Questions?

- **Email:** madhureddymandhala@gmail.com
- **GitHub Discussions:** [Community Discussions](https://github.com/madhureddy809662/Data-Enginnering/discussions)
- **LinkedIn:** [Connect on LinkedIn](https://in.linkedin.com/in/madhusudan-reddy-m/)

---

## 🎯 First Time Contributing?

If you're new to open source:
1. Look for issues labeled `good-first-issue`
2. Comment on the issue you want to work on
3. Ask for help if needed
4. Submit your PR!

Every contribution, no matter how small, is valuable. Thank you for helping improve this project! 🙏

---

*Last Updated: May 2026*
