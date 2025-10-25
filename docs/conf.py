# docs/conf.py
import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

project = "kde-cpi"
author = "Jacob Bourne"
version = release = "0.1.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",  # <— add
    "sphinx.ext.napoleon",  # <— add (Google/NumPy docstrings)
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "sphinx.ext.coverage",
    "sphinx.ext.mathjax",
    "sphinx.ext.ifconfig",
    "sphinx.ext.viewcode",
    "sphinx.ext.githubpages",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
    "sphinx_inline_tabs",
    "myst_parser",
    "sphinxcontrib.mermaid",
]

# Autodoc / autosummary behavior
autosummary_generate = True
autodoc_typehints = "description"
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
    # Uncomment if useful:
    # "inherited-members": True,
    # "special-members": "__call__",
}

# MyST (Markdown) quality-of-life
myst_enable_extensions = ["colon_fence", "dollarmath", "deflist"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "furo"
html_static_path = ["_static"]

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}
todo_include_todos = True
