import pandas as pd
import matplotlib.pyplot as plt
from .data_extraction import extract_target_tags, classify_signals_with_order


def visualize_target_tags_multi_ordered(dfs, metadatas, target_tags, time_column='Date', df_labels=None):
    """
    여러 DataFrame의 지정된 태그들을 DIO와 아날로그로 분류하여 가시화 (순서 유지)

    Parameters:
    - dfs: DataFrame 리스트 또는 단일 DataFrame
    - metadatas: 메타데이터 리스트 또는 단일 메타데이터
    - target_tags: 대상 태그 리스트 (순서 중요)
    - time_column: 시간 컬럼명
    - df_labels: DataFrame 라벨 리스트
    """
    # 단일 입력인 경우 리스트로 변환
    if isinstance(dfs, pd.DataFrame):
        dfs = [dfs]
    if isinstance(metadatas, dict):
        metadatas = [metadatas]

    # DataFrame 라벨 설정
    if df_labels is None:
        df_labels = [f'DF{i+1}' for i in range(len(dfs))]

    # 각 DataFrame에서 태그 추출 및 병합
    all_extracted_dfs = []
    all_tag_descriptions = {}

    for df_idx, (df, metadata) in enumerate(zip(dfs, metadatas)):
        # 대상 태그들 추출
        extracted_df, found_tags = extract_target_tags(df, metadata, target_tags)

        if not extracted_df.empty:
            # 시간 컬럼 추가
            if time_column in df.columns:
                extracted_df[time_column] = df[time_column]
            all_extracted_dfs.append(extracted_df)

            # 태그 설명 딕셔너리 생성 (첫 번째 DataFrame 기준)
            if df_idx == 0:
                tag_names = metadata['tag_names']
                column_names = metadata['column_names']

                for tag in found_tags:
                    for i, meta_tag in enumerate(tag_names):
                        if meta_tag == tag and i < len(column_names):
                            all_tag_descriptions[tag] = column_names[i]
                            break

    if not all_extracted_dfs:
        print("추출된 데이터가 없습니다.")
        return None, None

    # 공통 태그 찾기 (모든 DataFrame에 존재하는 태그)
    common_tags = set(all_extracted_dfs[0].columns)
    for extracted_df in all_extracted_dfs[1:]:
        common_tags = common_tags.intersection(set(extracted_df.columns))

    # 시간 컬럼 제외
    if time_column in common_tags:
        common_tags.remove(time_column)

    common_tags = list(common_tags)
    print(f"공통 태그: {len(common_tags)}개")

    if not common_tags:
        print("공통 태그가 없습니다.")
        return None, None

    # DIO와 아날로그 분류 (순서 유지)
    dio_signals, analog_signals = classify_signals_with_order(
        all_extracted_dfs[0], common_tags, target_tags
    )

    print(f"\n분류 결과:")
    print(f"DIO 신호: {len(dio_signals)}개")
    print(f"아날로그 신호: {len(analog_signals)}개")

    # 각각 가시화
    dio_fig = plot_dio_signals_ordered(all_extracted_dfs, dio_signals, time_column, all_tag_descriptions, df_labels)
    analog_fig = plot_analog_signals_ordered(all_extracted_dfs, analog_signals, time_column, all_tag_descriptions, df_labels)

    return dio_fig, analog_fig


def plot_dio_signals_ordered(dfs, dio_signals, time_column='Date', tag_descriptions=None, df_labels=None):
    """
    DIO 신호들을 순서대로 가시화 (여러 DataFrame 지원)

    Parameters:
    - dfs: DataFrame 또는 DataFrame 리스트
    - dio_signals: DIO 신호 리스트 (이미 순서가 정렬됨)
    - time_column: 시간 컬럼명 (표시용, 실제 X축은 인덱스 사용)
    - tag_descriptions: 태그 설명 딕셔너리
    - df_labels: DataFrame 라벨 리스트 (예: ['DF1', 'DF2'])
    """
    if not dio_signals:
        print("DIO 신호가 없습니다.")
        return None

    # 단일 DataFrame인 경우 리스트로 변환
    if isinstance(dfs, pd.DataFrame):
        dfs = [dfs]

    # DataFrame 라벨 설정
    if df_labels is None:
        df_labels = [f'DF{i+1}' for i in range(len(dfs))]

    # 색상과 스타일 설정
    colors = ['red', 'blue', 'green', 'purple', 'orange', 'brown']
    line_styles = ['-', '--', '-.', ':', '-', '--']

    # 플롯 생성
    n_signals = len(dio_signals)
    fig_height = max(6, n_signals * 0.8)

    fig, axes = plt.subplots(n_signals, 1, figsize=(15, fig_height), sharex=True)
    if n_signals == 1:
        axes = [axes]

    plt.suptitle('DIO 신호 모니터링 (다중 DataFrame)', fontsize=16, fontweight='bold')

    # dio_signals는 이미 원본 순서대로 정렬되어 있음
    for i, signal in enumerate(dio_signals):
        ax = axes[i]

        # 각 DataFrame에 대해 플롯
        for df_idx, df in enumerate(dfs):
            if signal not in df.columns:
                continue

            # X축은 인덱스 사용 (시간 정보 제거)
            x_data = range(len(df))

            # 아날로그 데이터 변환
            if isinstance(df[signal], pd.Series):
                signal_data = df[signal]
            elif isinstance(df[signal], pd.DataFrame):
                signal_data = df[signal].iloc[:, 0]
            else:
                signal_data = pd.Series(df[signal])

            dio_data = signal_data.map({'ON': 1, 'OFF': 0, '1': 1, '0': 0, 1: 1, 0: 0, True: 1, False: 0})
            dio_data = pd.to_numeric(dio_data, errors='coerce')

            # 계단형 플롯
            color = colors[df_idx % len(colors)]
            linestyle = line_styles[df_idx % len(line_styles)]

            ax.step(x_data, dio_data, where='post', linewidth=1.5,
                    color=color, linestyle=linestyle,
                    label=f'{df_labels[df_idx]}', alpha=0.8)

        # Y축 설정
        ax.set_ylim(-0.1, 1.1)
        ax.set_yticks([0, 1])
        ax.set_yticklabels(['OFF', 'ON'])

        # 격자 및 레이블
        ax.grid(True, alpha=0.3)
        ax.set_ylabel(signal, fontsize=11, rotation=0, ha='right', va='center')
        ax.tick_params(labelsize=8)

        # 태그 설명 추가 (왼쪽 상단)
        if tag_descriptions and signal in tag_descriptions:
            description = tag_descriptions[signal]
            ax.text(0.01, 0.95, description, transform=ax.transAxes,
                    ha='left', va='top', fontsize=10, fontweight='normal',
                    bbox=dict(boxstyle='round,pad=0.2', facecolor='lightyellow', alpha=0.8))

        # 범례 추가 (오른쪽 상단)
        if len(dfs) > 1:
            ax.legend(loc='upper right', fontsize=7, framealpha=0.8)

        # 현재 상태 표시 (각 DataFrame별)
        for df_idx, df in enumerate(dfs):
            if signal in df.columns:
                current_val = df[signal].iloc[-1]
                current_state = 'ON' if str(current_val).upper() in ['ON', '1', 'TRUE'] else 'OFF'

                # Y 위치를 DataFrame 개수에 따라 조정
                y_pos = 0.7 - (df_idx * 0.15)
                color = colors[df_idx % len(colors)]

                ax.text(0.99, y_pos, f'{df_labels[df_idx]}: {current_state}',
                        transform=ax.transAxes, ha='right', va='center',
                        fontsize=7, fontweight='bold',
                        bbox=dict(boxstyle='round,pad=0.2', facecolor=color, alpha=0.3))

    # X축 설정 - 인덱스 기반
    axes[-1].set_xlabel('데이터 포인트 (인덱스)', fontsize=12)

    # X축 틱을 적절히 조정
    if len(dfs) > 0:
        max_len = max(len(df) for df in dfs)
        if max_len > 100:
            # 데이터가 많으면 틱 간격 조정
            tick_interval = max_len // 10
            axes[-1].set_xticks(range(0, max_len, tick_interval))

    plt.tight_layout()
    return fig


def plot_analog_signals_ordered(dfs, analog_signals, time_column='Date', tag_descriptions=None, df_labels=None):
    """
    아날로그 신호들을 순서대로 가시화 (여러 DataFrame 지원)
    """
    if not analog_signals:
        print("아날로그 신호가 없습니다.")
        return None

    # 단일 DataFrame인 경우 리스트로 변환
    if isinstance(dfs, pd.DataFrame):
        dfs = [dfs]

    # DataFrame 라벨 설정
    if df_labels is None:
        df_labels = [f'DF{i+1}' for i in range(len(dfs))]

    # 색상과 스타일 설정
    colors = ['darkgreen', 'darkblue', 'darkred', 'purple', 'orange', 'brown']
    line_styles = ['-', '--', '-.', ':', '-', '--']

    # 플롯 생성
    n_signals = len(analog_signals)
    fig_height = max(8, n_signals * 1.2)

    fig, axes = plt.subplots(n_signals, 1, figsize=(15, fig_height), sharex=True)
    if n_signals == 1:
        axes = [axes]

    plt.suptitle('아날로그 신호 모니터링 (다중 DataFrame)', fontsize=16, fontweight='bold')

    # analog_signals는 이미 원본 순서대로 정렬되어 있음
    for i, signal in enumerate(analog_signals):
        ax = axes[i]

        # 각 DataFrame에 대해 플롯
        for df_idx, df in enumerate(dfs):
            if signal not in df.columns:
                continue

            # X축은 인덱스 사용 (시간 정보 제거)
            x_data = range(len(df))

            # 아날로그 데이터 변환
            if isinstance(df[signal], pd.Series):
                signal_data = df[signal]
            elif isinstance(df[signal], pd.DataFrame):
                signal_data = df[signal].iloc[:, 0]
            else:
                signal_data = pd.Series(df[signal])

            analog_data = pd.to_numeric(signal_data, errors='coerce')

            # 연속형 플롯
            color = colors[df_idx % len(colors)]
            linestyle = line_styles[df_idx % len(line_styles)]

            ax.plot(x_data, analog_data, linewidth=1.5,
                    color=color, linestyle=linestyle,
                    label=f'{df_labels[df_idx]}', alpha=0.8)

        # 격자 및 레이블
        ax.grid(True, alpha=0.3)
        ax.set_ylabel(signal, fontsize=11, rotation=0, ha='right', va='center')
        ax.tick_params(labelsize=8)

        # 태그 설명 추가 (왼쪽 상단)
        if tag_descriptions and signal in tag_descriptions:
            description = tag_descriptions[signal]
            ax.text(0.01, 0.95, description, transform=ax.transAxes,
                    ha='left', va='top', fontsize=10, fontweight='normal',
                    bbox=dict(boxstyle='round,pad=0.2', facecolor='lightyellow', alpha=0.8))

        # 범례 추가 (오른쪽 상단)
        if len(dfs) > 1:
            ax.legend(loc='upper right', fontsize=7, framealpha=0.8)

    # X축 설정 - 인덱스 기반
    axes[-1].set_xlabel('데이터 포인트 (인덱스)', fontsize=12)

    # X축 틱을 적절히 조정
    if len(dfs) > 0:
        max_len = max(len(df) for df in dfs)
        if max_len > 100:
            # 데이터가 많으면 틱 간격 조정
            tick_interval = max_len // 10
            axes[-1].set_xticks(range(0, max_len, tick_interval))

    plt.tight_layout()
    return fig
