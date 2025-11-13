import pandas as pd


def extract_target_tags(df, metadata, target_tags):
    """
    지정된 태그들만 추출하여 반환 (개선된 버전)
    컬럼명과 태그명의 길이 불일치 문제 해결
    """
    tag_names = metadata['tag_names']
    column_names = metadata['column_names']

    # 디버깅 정보 출력
    print(f"태그명 리스트 길이: {len(tag_names)}")
    print(f"컬럼명 리스트 길이: {len(column_names)}")
    print(f"DataFrame 컬럼 수: {len(df.columns)}")

    # 길이 불일치 경고
    if len(tag_names) != len(column_names):
        print(f"⚠️  길이 불일치 발견: 태그명 {len(tag_names)}개 vs 컬럼명 {len(column_names)}개")
        min_len = min(len(tag_names), len(column_names), len(df.columns))
        print(f"  → 안전한 길이 {min_len}로 제한하여 처리")
        tag_names = tag_names[:min_len]
        column_names = column_names[:min_len]

    # 결과 저장용
    found_columns = []
    found_tags = []
    missing_tags = []
    duplicate_tags = []

    # 태그-컬럼 매핑 딕셔너리 생성 (안전한 범위 내에서)
    safe_len = min(len(tag_names), len(column_names), len(df.columns))
    tag_to_column = {}
    duplicate_positions = {}  # 중복 위치 추적

    for i in range(safe_len):
        tag = str(tag_names[i]).strip()  # 문자열 변환 및 공백 제거
        col = str(column_names[i]).strip()  # 문자열 변환 및 공백 제거

        # 빈 태그는 스킫
        if tag and tag != 'nan' and tag != '':
            if tag in tag_to_column:
                # 중복 태그 발견
                if tag not in duplicate_positions:
                    duplicate_positions[tag] = [tag_to_column[tag]]
                duplicate_positions[tag].append(i)
            else:
                tag_to_column[tag] = i

    # 중복 태그 리스트 생성
    for tag, positions in duplicate_positions.items():
        duplicate_tags.append((tag, positions, positions))

    print(f"유효한 태그-컬럼 매핑: {len(tag_to_column)}개")

    # 중복 방지를 위한 집합 사용
    found_columns_set = set()
    found_tags_unique = []
    found_columns_unique = []

    # 타겟 태그들 처리
    for target_tag in target_tags:
        target_tag_clean = str(target_tag).strip()

        if target_tag_clean in tag_to_column:
            # 매칭되는 태그 찾음
            position = tag_to_column[target_tag_clean]

            # DataFrame에 해당 컬럼이 존재하는지 확인
            if position < len(df.columns):
                actual_column_name = df.columns[position]

                # 중복 체크 - 같은 컬럼이 이미 선택되었는지 확인
                if actual_column_name not in found_columns_set:
                    found_columns_set.add(actual_column_name)
                    found_columns_unique.append(actual_column_name)
                    found_tags_unique.append(target_tag_clean)

                    # 컬럼명과 메타데이터 컬럼명이 다른 경우 경고
                    expected_column = column_names[position] if position < len(column_names) else "Unknown"
                else:
                    print(f"⚠️  중복 컬럼 스킵: '{target_tag_clean}' -> '{actual_column_name}' (이미 선택됨)")

            else:
                print(f"❌ 태그 '{target_tag_clean}' 위치({position})가 DataFrame 범위를 벗어남")
                missing_tags.append(target_tag_clean)
        else:
            # 매칭되는 태그가 없음
            missing_tags.append(target_tag_clean)

            # 유사한 태그 찾기 (선택사항)
            similar_tags = [tag for tag in tag_to_column.keys() if target_tag_clean.lower() in tag.lower() or tag.lower() in target_tag_clean.lower()]
            if similar_tags:
                print(f"💡 '{target_tag_clean}' 유사 태그: {similar_tags[:3]}")  # 최대 3개까지만 표시

    # 중복 제거된 리스트 사용
    found_columns = found_columns_unique
    found_tags = found_tags_unique

    # 결과 출력
    print(f"\n태그 매칭 결과: {len(found_tags)}/{len(target_tags)} 개 찾음")
    print(f"중복 제거 후 실제 추출: {len(found_columns)}개 컬럼")

    if missing_tags:
        print(f"❌ 없는 태그 ({len(missing_tags)}개): {missing_tags}")

    # 데이터 추출
    if not found_columns:
        print("❌ 추출할 데이터가 없습니다.")
        return pd.DataFrame(), []

    try:
        # DataFrame 생성 시 안전한 처리
        print(f"\nDataFrame에서 {len(found_columns)}개 컬럼 추출 중...")

        # 실제 DataFrame 컬럼명 확인 및 매칭
        actual_found_columns = []
        actual_found_tags = []

        for i, (target_column, target_tag) in enumerate(zip(found_columns, found_tags)):
            # DataFrame에서 정확히 일치하는 컬럼 찾기
            if target_column in df.columns:
                actual_found_columns.append(target_column)
                actual_found_tags.append(target_tag)
            else:
                # 정확히 일치하지 않으면 유사한 컬럼 찾기 (pandas가 자동으로 이름을 바꾼 경우)
                similar_columns = [col for col in df.columns if target_column in col or col.startswith(target_column)]
                if similar_columns:
                    # 가장 유사한 첫 번째 컬럼 사용
                    selected_column = similar_columns[0]
                    actual_found_columns.append(selected_column)
                    actual_found_tags.append(target_tag)
                    print(f"🔄 컬럼명 대체: '{target_column}' -> '{selected_column}'")
                else:
                    print(f"❌ 컬럼을 찾을 수 없음: '{target_column}'")

        if not actual_found_columns:
            print("❌ 추출할 수 있는 컬럼이 없습니다.")
            return pd.DataFrame(), []

        print(f"📋 실제 추출할 컬럼 수: {len(actual_found_columns)}개")

        # DataFrame에서 실제 존재하는 컬럼들만 추출
        extracted_df = df[actual_found_columns].copy()

        # 컬럼명을 태그명으로 변경
        extracted_df.columns = actual_found_tags

        print(f"✅ 최종 추출 완료: {len(extracted_df.columns)}개 컬럼, {len(extracted_df)}개 행")

    except Exception as e:
        print(f"❌ 데이터 추출 중 오류: {e}")
        print(f"시도한 컬럼들: {found_columns}")
        print(f"DataFrame 컬럼들: {list(df.columns[:10])}...")  # 처음 10개만

        # 추가 디버깅 정보
        print(f"\n🔍 DataFrame 컬럼명 분석:")
        duplicate_names = {}
        for col in df.columns:
            base_name = col.split('.')[0]  # pandas가 추가한 접미사 제거
            if base_name in duplicate_names:
                duplicate_names[base_name].append(col)
            else:
                duplicate_names[base_name] = [col]

        # 중복된 컬럼명들 출력
        for base_name, variations in duplicate_names.items():
            if len(variations) > 1:
                print(f"  '{base_name}': {variations}")

        return pd.DataFrame(), []

    return extracted_df, found_tags


def classify_signals_with_order(df, signal_names, original_order):
    """
    신호를 DIO와 아날로그로 분류하면서 원본 순서 유지

    Parameters:
    - df: DataFrame
    - signal_names: 신호 이름 리스트
    - original_order: 원본 순서 리스트 (step_tags 등)
    """
    dio_signals = []
    analog_signals = []

    # 원본 순서를 유지하면서 분류
    for signal in original_order:
        if signal in signal_names and signal in df.columns:
            try:
                # DataFrame에서 해당 컬럼 추출 (Series로 확실히 변환)
                if isinstance(df[signal], pd.Series):
                    signal_data = df[signal]
                elif isinstance(df[signal], pd.DataFrame):
                    signal_data = df[signal].iloc[:, 0]
                else:
                    signal_data = pd.Series(df[signal])

                unique_vals = signal_data.dropna().unique()
                unique_count = len(unique_vals)

                # DIO 판별 로직
                if unique_count <= 2:
                    # ON/OFF, 0/1 등 확인
                    str_vals = set(str(v).upper() for v in unique_vals)
                    if str_vals.issubset({'ON', 'OFF', '0', '1', '0.0', '1.0', 'TRUE', 'FALSE'}):
                        dio_signals.append(signal)
                    else:
                        analog_signals.append(signal)
                else:
                    # 3개 이상의 고유값을 가지면 아날로그로 분류
                    try:
                        pd.to_numeric(df[signal].dropna())
                        analog_signals.append(signal)
                    except:
                        # 숫자가 아니면 DIO로 분류 (예외적인 경우)
                        dio_signals.append(signal)
            except Exception as e:
                print(f"⚠️ 신호 '{signal}' 처리 중 오류: {e}")
                analog_signals.append(signal)

    return dio_signals, analog_signals
