# app.py
from flask import Flask, render_template, request, send_file
import os
from werkzeug.utils import secure_filename
from your_script import process_all

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'output'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        uploaded_files = request.files.getlist("json_files")
        for file in os.listdir(UPLOAD_FOLDER):
            os.remove(os.path.join(UPLOAD_FOLDER, file))

        for file in uploaded_files:
            if file.filename.endswith('.json'):
                filepath = os.path.join(UPLOAD_FOLDER, secure_filename(file.filename))
                file.save(filepath)

        output_excel = os.path.join(OUTPUT_FOLDER, "cumulative_output.xlsx")
        output_csv = os.path.join(OUTPUT_FOLDER, "cumulative_output.csv")

        process_all(UPLOAD_FOLDER, output_csv, output_excel, skip_excel=False)
        return send_file(output_excel, as_attachment=True)

    return render_template('index.html')

if __name__ == '__main__':
 app.run(host="0.0.0.0", port=10000)