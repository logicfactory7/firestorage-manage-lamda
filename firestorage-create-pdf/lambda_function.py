import json
import io
from spire.doc import Document
from spire.doc import FileFormat

# pip3 install --platform manylinux2014_x86_64 --only-binary=:all:  weasyprint -t .
# pip3 install --platform manylinux2014_x86_64 --only-binary=:all:  cffi -t .

# pip3 install --platform manylinux2014_x86_64 --target=package --implementation cp --python-version 3.12 --only-binary=:all: --upgrade Spire.Doc -t .
# pip3 install --platform manylinux2014_x86_64 --target=package --implementation cp --python-version 3.12 --only-binary=:all: --upgrade PyICU -t .



def lambda_handler(event, context):
    # TODO implement
    doc = Document()

    # セクションを追加します
    sec = doc.AddSection()

    # 段落を追加します
    par = sec.AddParagraph()

    template = """
<!DOCTYPE html>
<html>
<body>
    <h1>Example with Hyperlinks</h1>
    <p>Visit <a href="https://www.example.com">Example</a></p>
</body>
</html>
    """

    par.AppendHTML(template)

    # ドキュメントを PDF ファイルとして保存します
#    doc.SaveToFile("output/HTML文字列をPDFに変換.pdf", FileFormat.PDF)
    doc.Close()

    return {
        'statusCode': 200,
        'body': json.dumps("OK")
    }
