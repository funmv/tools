import json
import tables
import pandas as pd

# ============================================================================
# HDF5 로드 및 메타데이터 처리 함수
# ============================================================================

def load_hdf5_with_metadata(file_path):
    """
    HDF5 파일에서 DataFrame과 메타데이터 로드
    
    Args:
        file_path: HDF5 파일 경로
    
    Returns:
        df: DataFrame (attrs에 메타데이터 포함)
    """
    print(f"{'='*60}")
    print(f"HDF5 파일 로드: {file_path}")
    print(f"{'='*60}")
    
    try:
        # DataFrame 로드
        df = pd.read_hdf(file_path, key='data')
        print(f"\n✅ DataFrame 로드 완료")
        print(f"   Shape: {df.shape}")
        print(f"   Columns: {len(df.columns)}개")
        
        # 메타데이터 로드
        try:
            with tables.open_file(file_path, 'r') as h5file:
                group = h5file.get_node('/data')
                
                if hasattr(group._v_attrs, 'pandas_attrs'):
                    attrs_json = group._v_attrs.pandas_attrs
                    df.attrs = json.loads(attrs_json)
                    
                    print(f"\n✅ 메타데이터 로드 완료")
                    print(f"   메타데이터 키: {list(df.attrs.keys())}")
                    
                    # header_metadata를 파싱하여 컬럼별 매핑 생성
                    if 'header_metadata' in df.attrs:
                        df.attrs['_column_mapping'] = build_column_mapping(df)
                else:
                    print(f"\n⚠️ pandas_attrs가 없습니다.")
        
        except Exception as e:
            print(f"\n⚠️ 메타데이터 로드 실패: {e}")
        
        return df
    
    except Exception as e:
        print(f"\n❌ 파일 로드 실패: {e}")
        import traceback
        print(traceback.format_exc())
        return None

def build_column_mapping(df):
    """
    header_metadata에서 컬럼별 매핑 딕셔너리 생성
    
    Args:
        df: DataFrame (attrs 포함)
    
    Returns:
        dict: {tag_name: column_name, ...}
    """
    mapping = {}
    
    if not hasattr(df, 'attrs') or 'header_metadata' not in df.attrs:
        return mapping
    
    header_meta = df.attrs['header_metadata']
    
    # header_metadata가 딕셔너리이고 tag_name 키가 있는 경우
    if isinstance(header_meta, dict) and 'tag_name' in header_meta:
        tag_names = header_meta['tag_name']
        
        # tag_name이 리스트이고 컬럼과 같은 길이인 경우
        if isinstance(tag_names, list) and len(tag_names) == len(df.columns):
            for col, tag in zip(df.columns, tag_names):
                if pd.notna(tag) and tag != '':  # nan이 아닌 경우만
                    mapping[tag] = col
    
    return mapping

def print_metadata_summary(df):
    """
    메타데이터 요약 정보 출력
    
    Args:
        df: DataFrame (attrs 포함)
    """
    print(f"\n{'='*60}")
    print(f"메타데이터 요약")
    print(f"{'='*60}")
    
    if not hasattr(df, 'attrs') or not df.attrs:
        print("\n⚠️ 메타데이터가 없습니다.")
        return
    
    for key, value in df.attrs.items():
        if key == '_column_mapping':
            # 내부 매핑은 표시하지 않음
            continue
        elif key == 'header_metadata':
            print(f"\n📋 {key}:")
            if isinstance(value, dict):
                for meta_key, meta_value in value.items():
                    if isinstance(meta_value, list):
                        # 리스트의 처음 5개만 표시
                        sample = [v for v in meta_value[:10] if pd.notna(v) and v != ''][:5]
                        print(f"   {meta_key}: {sample}{'...' if len(meta_value) > 10 else ''}")
                    else:
                        print(f"   {meta_key}: {meta_value}")
        elif isinstance(value, dict):
            print(f"\n📋 {key} ({len(value)}개):")
            # 처음 3개만 샘플로 표시
            for i, (tag, info) in enumerate(list(value.items())[:3]):
                info_str = str(info)[:50] + '...' if len(str(info)) > 50 else str(info)
                print(f"   {tag}: {info_str}")
            
            if len(value) > 3:
                print(f"   ... (총 {len(value)}개)")
        elif isinstance(value, list):
            print(f"\n📋 {key}: {value[:5]}{'...' if len(value) > 5 else ''}")
        else:
            print(f"\n📋 {key}: {value}")
    
    # 매핑 정보 출력
    if '_column_mapping' in df.attrs:
        mapping = df.attrs['_column_mapping']
        print(f"\n📋 태그-컬럼 매핑: {len(mapping)}개")
        # 샘플 3개만 표시
        for i, (tag, col) in enumerate(list(mapping.items())[:3]):
            print(f"   {tag} → {col}")
        if len(mapping) > 3:
            print(f"   ... (총 {len(mapping)}개)")