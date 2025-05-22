import re


def clean_text(text: str) -> str:
    # 페이지/헤더/푸터 제거
    text = re.sub(r"Page\s+\d+.*", "", text)
    text = re.sub(r"arXiv:\d{4}\.\d{4,5}(v\d+)?", "", text)

    # 하이픈 줄바꿈 및 단어 줄바꿈 연결
    text = re.sub(r"(?<=\w)-\n(?=\w)", "", text)
    text = re.sub(r"(?<=\w)\n(?=\w)", " ", text)

    # figure, table 라벨 제거
    text = re.sub(r"(Figure|Table) \d+:.*?(?=\n|$)", "", text)

    # section 번호 제거 (1, 2.1, 3.2.4 ...)
    text = re.sub(r"^\s*\d+(\.\d+)*\s+", "", text, flags=re.MULTILINE)

    # 이름/소속/이메일 제거
    text = re.sub(r"(?i)([a-z]+\s){2,7}((UC|MIT|Stanford|Berkeley|Google|Facebook|AI|Dept)[^\n]*)", "", text)

    # 줄바꿈 정리
    text = re.sub(r"\n+", " ", text)
    text = re.sub(r"\s+", " ", text)

    return text.strip()
