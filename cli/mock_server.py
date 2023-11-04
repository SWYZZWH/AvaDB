from flask import Flask, Response
import io

app = Flask(__name__)


# This route will match any URL
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def get_file(path):

    def generate_file():
        for i in range(1000):
            yield f"This is line {i} with some content, including spaces and newlines."

    # Create a generator and wrap it in a BytesIO buffer
    file_generator = generate_file()

    # The response will stream the content of the generator
    return Response(
        io.BytesIO('\n'.join(file_generator).encode('utf-8')),
        mimetype='text/plain',
        headers={"Content-Disposition": "attachment;filename=download.txt"}
    )


if __name__ == '__main__':
    app.run(port=9527)
