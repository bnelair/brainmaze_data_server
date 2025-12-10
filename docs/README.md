# Documentation

This directory contains the Sphinx documentation for the bnel-mef3-server project.

## Building Documentation Locally

1. Install documentation dependencies:
   ```bash
   pip install -e ".[docs]"
   ```

2. Build the HTML documentation:
   ```bash
   cd docs
   make html
   ```

   Or alternatively:
   ```bash
   sphinx-build -b html docs/ docs/_build/html
   ```

3. View the documentation:
   Open `docs/_build/html/index.html` in your web browser.

## Automatic Deployment

Documentation is automatically built and deployed to GitHub Pages whenever changes are pushed to the `main` branch. The GitHub Actions workflow (`.github/workflows/docs.yml`) handles:

1. Installing dependencies
2. Building the Sphinx documentation
3. Deploying to the `gh-pages` branch
4. Publishing to GitHub Pages

The live documentation is available at: https://bnelair.github.io/brainmaze_data_server/

## Documentation Structure

- `conf.py` - Sphinx configuration file
- `index.rst` - Main documentation page
- `api/` - API reference documentation for each module
- `_static/` - Static files (CSS, images, etc.)
- `_templates/` - Custom Sphinx templates
- `_build/` - Generated documentation (gitignored)

## Docstring Style

This project uses Google-style docstrings. All public APIs should be documented following the Google Python Style Guide conventions. Sphinx uses the Napoleon extension to parse these docstrings.

Example:
```python
def example_function(param1, param2):
    """Brief description of the function.

    Longer description if needed.

    Args:
        param1 (str): Description of param1.
        param2 (int): Description of param2.

    Returns:
        bool: Description of return value.

    Raises:
        ValueError: Description of when this error is raised.
    """
    pass
```
