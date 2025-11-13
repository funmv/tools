import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates
from pathlib import Path

from utils.load_file import load_hdf5_with_metadata, print_metadata_summary
from utils.step_tags import step_tags, target_tags
from utils.data_extraction import extract_target_tags, classify_signals_with_order
from utils.visualization import visualize_target_tags_multi_ordered, plot_dio_signals_ordered, plot_analog_signals_ordered


# ============================================================================
# 한글 폰트 설정
# ============================================================================
plt.rcParams['font.family'] = 'DejaVu Sans'
try:
    # Windows 환경에서 한글 폰트 설정
    import platform
    if platform.system() == 'Windows':
        plt.rcParams['font.family'] = 'Malgun Gothic'
    else:
        # Mac/Linux 환경
        plt.rcParams['font.family'] = 'AppleGothic'
except:
    # 폰트 설정 실패시 경고 무시
    import warnings
    warnings.filterwarnings('ignore', category=UserWarning, module='matplotlib')
    
plt.rcParams['axes.unicode_minus'] = False  # 마이너스 기호 깨짐 방지


# ============================================================================
# Main 함수부
# ============================================================================
h5_file_path = "output_data_yw_YW-DATA_short_2024-08.h5"
    
# 파일 로드
df = load_hdf5_with_metadata(h5_file_path)
print_metadata_summary(df)


print(df.attrs.keys(), df.attrs['header_metadata']['description'][:10], df.attrs['header_metadata']['tag_name'][:10])


metadata={}
metadata['column_names'] = df.attrs['header_metadata']['description'][1:].copy()
metadata['tag_names'] = df.attrs['header_metadata']['tag_name'][1:].copy()
print(metadata['column_names'][155:158], metadata['tag_names'][155:158], df.columns[155:158], df.columns[:5], metadata['column_names'][:5])




# ============================================================================
# 플롯 저장 설정
# ============================================================================
output_dir = Path('output_plots')
output_dir.mkdir(exist_ok=True)
dpi = 150
bbox = 'tight'

print(f"\n{'='*70}")
print(f"플롯을 '{output_dir}' 폴더에 저장합니다.")
print(f"{'='*70}\n")

# ============================================================================
# Step별 플롯 생성 및 저장
# ============================================================================
for ii in range(10):
    print(f"{'='*70}")
    print(f"Step {ii+1:02d} 처리 중...")
    print(f"{'='*70}")

    target_tags = step_tags[ii]

    # 여러 DataFrame 동시 가시화
    dio_fig, analog_fig = visualize_target_tags_multi_ordered(
        dfs=[df],
        metadatas=[metadata],
        target_tags=target_tags,
        df_labels=['2024-08']
    )

    # DIO 플롯 저장
    if dio_fig is not None:
        dio_path = output_dir / f'step{ii+1:02d}_dio.png'
        dio_fig.savefig(dio_path, dpi=dpi, bbox_inches=bbox)
        print(f"  ✅ DIO 저장: {dio_path}")
    else:
        print(f"  ⚠️  DIO 플롯이 생성되지 않았습니다.")

    # 아날로그 플롯 저장
    if analog_fig is not None:
        analog_path = output_dir / f'step{ii+1:02d}_analog.png'
        analog_fig.savefig(analog_path, dpi=dpi, bbox_inches=bbox)
        print(f"  ✅ 아날로그 저장: {analog_path}")
    else:
        print(f"  ⚠️  아날로그 플롯이 생성되지 않았습니다.")

    # 메모리 절약
    plt.close('all')
    print()

print(f"{'='*70}")
print(f"✅ 모든 플롯이 '{output_dir}' 폴더에 저장되었습니다.")
print(f"{'='*70}")

