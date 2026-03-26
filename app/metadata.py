"""
Metadata loader for Directum RX / Sungero entity definitions.
Maps processId (NameGuid) → human-readable names.
"""

import json
from pathlib import Path


def _load_json(filepath: Path) -> list:
    try:
        with open(filepath, encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def load_process_names(metadata_dir: str) -> dict[str, dict]:
    """
    Load all metadata JSON files and build a mapping:
        processId (lower-case GUID) → {name, display_name}

    Only entities with a NameGuid are included (workflow task entities).
    """
    mapping: dict[str, dict] = {}

    for mf in sorted(Path(metadata_dir).glob('*.json')):
        for item in _load_json(mf):
            guid = item.get('NameGuid', '').lower().strip()
            if not guid:
                continue
            name = item.get('Name', '')
            display = item.get('DisplayName_ru', '') or ''
            if guid not in mapping:
                mapping[guid] = {
                    'name': name,
                    'display_name': display if display else name,
                }

    return mapping


# ── Nice category labels based on entity name patterns ──────────────────────
_CATEGORY_RULES = [
    ('approval', 'Согласование'),
    ('acquaintance', 'Ознакомление'),
    ('action_item', 'Поручение'),
    ('vacation', 'Отпуск'),
    ('hiring', 'Прием сотрудника'),
    ('absence', 'Отсутствие'),
    ('business_trip', 'Командировка'),
    ('resignation', 'Увольнение'),
    ('hr', 'Кадровые процессы'),
    ('document', 'Работа с документами'),
    ('project', 'Проект'),
    ('request', 'Обращение'),
    ('simple', 'Простые задачи'),
    ('notification', 'Уведомления'),
    ('lead', 'Работа с лидами'),
    ('partner', 'Партнер'),
    ('deal', 'Сделка'),
    ('training', 'Обучение'),
    ('grade', 'Аттестация'),
    ('certificate', 'Сертификаты'),
    ('consent', 'Согласия'),
    ('pdf', 'Обработка PDF'),
    ('convert', 'Конвертация'),
    ('reserve', 'Резервирование'),
    ('staff', 'Кадровое перемещение'),
    ('plan', 'Планирование'),
    ('rate', 'Ставки'),
    ('candidate', 'Кандидаты'),
    ('presale', 'Пресейл'),
    ('map_approval', 'Карты целей'),
]


def get_category(process_name: str) -> str:
    name_lower = process_name.lower()
    for keyword, category in _CATEGORY_RULES:
        if keyword in name_lower:
            return category
    return 'Прочее'
