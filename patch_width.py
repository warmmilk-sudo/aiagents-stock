import os
import glob

def patch_streamlit_buttons(directory):
    for filepath in glob.glob(os.path.join(directory, '**/*.py'), recursive=True):
        if 'venv' in filepath or '.git' in filepath:
            continue
            
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # specifically button
        # find `st.button(..., width='stretch')` replace with `st.button(..., width="stretch")`
        new_content = content.replace('st.button(', 'temp_st_button_')
        # We need to be careful with replace, maybe better with regex
        
patch_streamlit_buttons('f:/zfywork/aiagents-stock')

import re
def regex_patch_buttons(directory):
    count = 0
    for filepath in glob.glob(os.path.join(directory, '**/*.py'), recursive=True):
        if 'venv' in filepath or '.git' in filepath or 'site-packages' in filepath:
            continue
            
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Pattern to match st.button calls with use_container_width=True
            # It's a bit tricky, but we can do a simpler replace because use_container_width=True might be alone.
            
            # Simple direct replace for typical patterns in this codebase
            changed = False
            
            # 1. button
            if 'st.button(' in content and 'use_container_width=' in content:
                new_content = re.sub(r'(st\.button\([^)]*)use_container_width=True([^)]*\))', r"\1width='stretch'\2", content)
                new_content = re.sub(r'(st\.button\([^)]*)use_container_width=False([^)]*\))', r"\1width='content'\2", new_content)
                if new_content != content:
                    content = new_content
                    changed = True
            
            # 2. dataframe (warning might only be for button, but let's leave dataframe alone)
            
            if changed:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"Patched: {filepath}")
                count += 1
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
            
    print(f"Total files patched: {count}")

regex_patch_buttons('f:/zfywork/aiagents-stock')
