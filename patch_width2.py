import os
import glob
import re

def regex_patch_buttons(directory):
    count = 0
    pattern_true = re.compile(r'(st\.button\([^)]*)use_container_width\s*=\s*True(.*?\))', re.DOTALL)
    pattern_false = re.compile(r'(st\.button\([^)]*)use_container_width\s*=\s*False(.*?\))', re.DOTALL)
    
    for filepath in glob.glob(os.path.join(directory, '**/*.py'), recursive=True):
        if 'venv' in filepath or '.git' in filepath or 'site-packages' in filepath:
            continue
            
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
                
            original_content = content
            
            # Simple direct replace for typical patterns in this codebase
            content = pattern_true.sub(r"\1width='stretch'\2", content)
            content = pattern_false.sub(r"\1width='content'\2", content)
            
            if content != original_content:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"Patched: {filepath}")
                count += 1
        except Exception as e:
            pass
            
    print(f"Total files patched: {count}")

regex_patch_buttons('f:/zfywork/aiagents-stock')
