# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "flask",
#     "markdown"
# ]
# ///
# github_readme.py
import socket
from flask import Flask, request, jsonify
import markdown

app = Flask(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Markdown Preview</title>
    <!-- GitHub-like CSS for styling -->
    <link rel="stylesheet"
          href="https://cdnjs.cloudflare.com/ajax/libs/github-markdown-css/5.1.0/github-markdown-light.min.css"
          integrity="sha512-S1JgCL4TnTF2+RWzx1JQNs6P8EvPMHk4fKJc1AIcvqF4GuqruGpJZn5wQ7NmVvrqfLqPS9YYdu0wUCSvFqBdAA=="
          crossorigin="anonymous" referrerpolicy="no-referrer" />
    <style>
      body {
        display: flex;
        margin: 20px;
        gap: 20px;
        font-family: Arial, sans-serif;
      }
      #markdown-input {
        width: 45%;
        height: 80vh;
        padding: 10px;
        font-size: 14px;
        font-family: monospace;
      }
      .markdown-body {
        width: 50%;
        padding: 20px;
        border: 1px solid #d1d5da;
        overflow-y: auto;
        max-height: 80vh;
      }
    </style>
</head>
<body>
    <textarea id="markdown-input" placeholder="Type or paste your Markdown here..."></textarea>
    <div id="preview" class="markdown-body"></div>
    <script>
        const mdInput = document.getElementById('markdown-input');
        const preview = document.getElementById('preview');

        mdInput.addEventListener('input', async () => {
            const response = await fetch('/preview', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ markdown: mdInput.value })
            });
            const data = await response.json();
            preview.innerHTML = data.html;
        });
    </script>
</body>
</html>
"""

def find_free_port(start=5000, limit=50):
    """
    Try to find a free port starting from `start`.
    We attempt up to `start + limit`.
    Returns the free port number or raises RuntimeError.
    """
    for port in range(start, start + limit):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # So we don't hang onto the port after checking
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            # Port is in use; move on
            s.close()
            continue
        s.close()
        return port

    raise RuntimeError("No free ports found in range.")

@app.route("/")
def index():
    """Serve the main HTML with the textarea and preview section."""
    return HTML_TEMPLATE

@app.route("/preview", methods=["POST"])
def preview():
    """Receive Markdown from the frontend and return rendered HTML."""
    content = request.get_json().get('markdown', '')
    rendered_html = markdown.markdown(content, extensions=['fenced_code', 'codehilite'])
    return jsonify({"html": rendered_html})

if __name__ == "__main__":
    port_to_use = find_free_port(start=5000, limit=50)
    print(f"Starting server on port {port_to_use}...")
    app.run(debug=True, port=port_to_use)