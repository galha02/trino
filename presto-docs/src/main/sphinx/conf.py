#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Presto documentation build configuration file
#
# This file is execfile()d with the current directory set to its containing dir.
#

import os
import sys
import xml.dom.minidom

try:
    sys.dont_write_bytecode = True
except:
    pass

sys.path.insert(0, os.path.abspath('ext'))


def child_node(node, name):
    for i in node.childNodes:
        if (i.nodeType == i.ELEMENT_NODE) and (i.tagName == name):
            return i
    return None


def node_text(node):
    return node.childNodes[0].data


def maven_version(pom):
    dom = xml.dom.minidom.parse(pom)
    project = dom.childNodes[0]

    version = child_node(project, 'version')
    if version:
        return node_text(version)

    parent = child_node(project, 'parent')
    version = child_node(parent, 'version')
    return node_text(version)


def get_version():
    version = os.environ.get('PRESTO_VERSION', '').strip()
    return version or maven_version('../../../pom.xml')

# -- General configuration -----------------------------------------------------

needs_sphinx = '2.0'

extensions = ['myst_parser', 'backquote', 'download', 'issue']

templates_path = ['templates']

source_suffix = '.rst'

master_doc = 'index'

project = u'Presto'

version = get_version()
release = version

exclude_patterns = ['_build']

highlight_language = 'sql'

default_role = 'backquote'

rst_epilog = """
.. |presto_server_release| replace:: ``presto-server-{release}``
""".replace('{release}', release)

# -- Options for HTML output ---------------------------------------------------

html_theme = 'sphinx_material'

html_static_path = ['static']

html_title = '%s %s Documentation' % (project, release)

html_logo = 'images/presto.svg'

html_add_permalinks = '#'
html_show_copyright = False
html_show_sphinx = False

html_sidebars = {
    "**": ['logo-text.html', 'globaltoc.html', 'localtoc.html', 'searchbox.html']
}

html_theme_options = {
    'base_url': '/',
    'globaltoc_depth': -1,
    'theme_color': '2196f3',
    'color_primary': 'teal',
    'color_accent': 'pink',
    'repo_url': 'https://github.com/prestosql/presto',
    'repo_name': 'Presto',
    'version_json': '../versions.json',
}

html_css_files = [
    'presto.css',
]
