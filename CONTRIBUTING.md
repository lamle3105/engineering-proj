# Contributing to Data Engineering Pipeline

Thank you for considering contributing to this project! This document provides guidelines for contributing.

## How to Contribute

### Reporting Bugs

If you find a bug, please create an issue with:
- Clear description of the bug
- Steps to reproduce
- Expected behavior
- Actual behavior
- Environment details (Python version, OS, etc.)

### Suggesting Enhancements

Enhancement suggestions are welcome! Please create an issue with:
- Clear description of the enhancement
- Use case and benefits
- Potential implementation approach (if applicable)

### Pull Requests

1. **Fork the repository**
   ```bash
   git clone https://github.com/lamle3105/engineering-proj.git
   cd engineering-proj
   ```

2. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make your changes**
   - Follow the existing code style
   - Add tests for new functionality
   - Update documentation as needed

4. **Test your changes**
   ```bash
   python tests/test_pipeline.py
   python scripts/validate_structure.py
   ```

5. **Commit your changes**
   ```bash
   git add .
   git commit -m "Add feature: description"
   ```

6. **Push to your fork**
   ```bash
   git push origin feature/your-feature-name
   ```

7. **Create a Pull Request**
   - Go to the original repository
   - Click "New Pull Request"
   - Select your feature branch
   - Provide a clear description of changes

## Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/lamle3105/engineering-proj.git
   cd engineering-proj
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Setup configuration**
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

## Code Style

### Python

- Follow PEP 8 guidelines
- Use meaningful variable and function names
- Add docstrings to functions and classes
- Keep functions focused and single-purpose
- Maximum line length: 100 characters

### Example

```python
def process_data(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Process and transform the input dataframe.
    
    Args:
        df: Input dataframe
        config: Configuration dictionary
        
    Returns:
        Processed dataframe
    """
    # Implementation
    pass
```

## Testing

### Unit Tests

- Write tests for all new functionality
- Use unittest or pytest framework
- Aim for >80% code coverage
- Test edge cases and error conditions

### Running Tests

```bash
python tests/test_pipeline.py
```

## Documentation

### Code Documentation

- Add docstrings to all modules, classes, and functions
- Use Google-style or NumPy-style docstrings
- Include type hints where applicable

### README Updates

- Update README.md if adding new features
- Include usage examples
- Update installation instructions if needed

### Documentation Files

Update relevant documentation in `docs/`:
- `ARCHITECTURE.md` for architectural changes
- `POWERBI_INTEGRATION.md` for BI-related changes
- `QUICKSTART.md` for setup process changes

## Project Structure

```
data_pipeline/
├── ingestion/      # Data ingestion from various sources
├── transformation/ # Spark transformations and star schema
├── masking/       # Data masking for PII protection
└── utils/         # Utility functions and helpers
```

### Adding New Components

#### New Data Source

1. Create a new file in `data_pipeline/ingestion/`
2. Inherit from `DataSource` base class
3. Implement `extract()` and `validate()` methods
4. Add configuration to `pipeline_config.yaml`
5. Update documentation

#### New Transformation

1. Add transformation to `data_pipeline/transformation/`
2. Use Spark DataFrame API
3. Add tests for the transformation
4. Update Airflow DAG if needed

## Commit Messages

Follow conventional commits format:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

Examples:
```
feat(ingestion): add support for MongoDB source
fix(masking): correct email masking regex pattern
docs(readme): update installation instructions
```

## Review Process

1. Pull requests require at least one approval
2. All tests must pass
3. Code must follow style guidelines
4. Documentation must be updated

## Questions?

Feel free to:
- Open an issue for questions
- Join our community discussions
- Contact the maintainers

## License

By contributing, you agree that your contributions will be licensed under the same license as the project (MIT License).

## Thank You!

Your contributions make this project better for everyone. Thank you for taking the time to contribute!
