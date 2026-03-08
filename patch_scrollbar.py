import sys

with open("f:/zfywork/aiagents-stock/app.py", "r", encoding="utf-8") as f:
    text = f.read()

target = '''    .page-title-wrap {
        text-align: center;
        margin-bottom: 1.1rem;
        padding-top: 0.3rem;
        overflow: visible;
    }'''

replacement = '''    .page-title-wrap {
        text-align: center;
        margin-bottom: 1.1rem;
        padding-top: 0.3rem;
        overflow: visible;
    }

    /* 隐藏全局垂直滚动条 */
    ::-webkit-scrollbar {
        width: 0px;
        background: transparent;
    }
    html, body {
        scrollbar-width: none;
        -ms-overflow-style: none;
    }'''

# Replace target if exists, try different line endings
target_rn = target.replace('\n', '\r\n')
text = text.replace(target, replacement).replace(target_rn, replacement)

with open("f:/zfywork/aiagents-stock/app.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Patched app.py to hide scrollbar")
