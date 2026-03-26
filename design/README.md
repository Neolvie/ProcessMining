# Directum Design System — краткое руководство для агентов

Этот каталог содержит описание дизайн-системы Directum, извлечённое с https://www.directum.ru/ui-kit,
для повторного использования в будущих задачах.

## Логотип

```
URL: https://www.directum.ru/application/images/directum_logo.png
Высота в хедере: 26 px
Резервный вариант: текст "Directum" в var(--orange)
```

HTML для вставки:
```html
<img src="https://www.directum.ru/application/images/directum_logo.png"
     alt="Directum"
     onerror="this.style.display='none'"/>
```

## Шрифт

- **Основной**: Inter (Google Fonts)
- **Резервный**: 'Segoe UI', system-ui, sans-serif
- Подключение: `https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap`

## Цветовая палитра

| Токен          | Значение  | Назначение                        |
|----------------|-----------|-----------------------------------|
| `--orange`     | `#FF7A00` | Основной брендовый, кнопки, акцент|
| `--orange-btn` | `#FF8F35` | Hover-состояние кнопок            |
| `--orange-light`| `#FFF3E4`| Фон оранжевых элементов           |
| `--blue`       | `#3C65CC` | Ссылки, вторичный акцент          |
| `--blue-light` | `#EEF2FF` | Фон синих элементов               |
| `--navy`       | `#000E20` | Тёмно-синий текст/заголовки       |
| `--title`      | `#05184A` | Заголовки страниц                 |
| `--bg`         | `#F4F4F4` | Фон страницы                      |
| `--surface`    | `#FFFFFF` | Фон карточек                      |
| `--surface2`   | `#F8F8F8` | Фон вторичных элементов           |
| `--border`     | `#E0E0E0` | Границы                           |
| `--text`       | `#000E20` | Основной текст                    |
| `--muted`      | `#625F6A` | Второстепенный текст              |
| `--subtle`     | `#9C9BA8` | Подсказки, метки                  |
| `--green`      | `#3AC436` | Успех                             |
| `--red`        | `#D32F2F` | Ошибка, прерывание                |
| `--amber`      | `#F5A623` | Предупреждение                    |

## Тени

```css
--shadow-sm:    0 1px 3px rgba(1,12,28,.07);
--shadow-md:    0 4px 12px rgba(1,12,28,.08);
--shadow-hover: 0 8px 24px rgba(1,12,28,.10);
```

## Скругления

```css
--radius:    10px;  /* карточки, модалки */
--radius-sm:  6px;  /* кнопки, бейджи */
```

## Типографика

```css
/* Заголовки */
font-size: 22px; font-weight: 700; color: var(--title);

/* Подзаголовки карточек */
font-size: 13px; font-weight: 700; color: var(--navy); text-transform: uppercase; letter-spacing: .06em;

/* Основной текст */
font-size: 14px; color: var(--text);

/* Мелкий вспомогательный */
font-size: 12px; color: var(--muted);
```

## Компоненты

### Кнопка (primary)
```css
.btn {
  background: var(--orange);
  color: #fff;
  border: none;
  border-radius: var(--radius-sm);
  padding: 8px 16px;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
  transition: background .15s;
}
.btn:hover { background: var(--orange-btn); }
```

### Кнопка (ghost)
```css
.btn-ghost {
  background: transparent;
  color: var(--muted);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 8px 16px;
  font-size: 13px;
  cursor: pointer;
}
.btn-ghost:hover { border-color: var(--orange); color: var(--orange); }
```

### Карточка
```css
.card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 20px;
  box-shadow: var(--shadow-sm);
  transition: box-shadow .2s;
}
.card:hover { box-shadow: var(--shadow-hover); }
```

### Навигация (sidebar)
```css
/* Активный элемент */
.nav-item.active {
  background: var(--orange-light);
  color: var(--orange);
  border-left: 3px solid var(--orange);
  font-weight: 600;
}
/* Обычный элемент */
.nav-item {
  display: flex; align-items: center; gap: 10px;
  padding: 10px 16px 10px 20px;
  border-radius: 0 8px 8px 0;
  cursor: pointer;
  color: var(--muted);
  transition: all .15s;
}
.nav-item:hover { background: var(--bg); color: var(--text); }
```

### Бейдж
```css
.badge { padding: 2px 8px; border-radius: 20px; font-size: 11px; font-weight: 600; }
.badge-green  { background: var(--green-light);  color: var(--green); }
.badge-red    { background: var(--red-light);    color: var(--red); }
.badge-amber  { background: var(--amber-light);  color: var(--amber); }
.badge-blue   { background: var(--blue-light);   color: var(--blue); }
.badge-muted  { background: var(--surface2);     color: var(--muted); }
```

### Бейдж статуса сортировки
```css
th.sort-asc::after  { content: ' ↑'; color: var(--orange); }
th.sort-desc::after { content: ' ↓'; color: var(--orange); }
```

### KPI-карточка
```css
.kpi {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 18px 20px;
  box-shadow: var(--shadow-sm);
}
.kpi-label { font-size: 12px; color: var(--muted); margin-bottom: 4px; }
.kpi-value { font-size: 26px; font-weight: 700; color: var(--text); }
.kpi-sub   { font-size: 12px; color: var(--subtle); margin-top: 4px; }

/* Цветные акценты — верхняя полоска */
.kpi.accent-orange { border-top: 3px solid var(--orange); }
.kpi.accent-green  { border-top: 3px solid var(--green); }
.kpi.accent-red    { border-top: 3px solid var(--red); }
.kpi.accent-amber  { border-top: 3px solid var(--amber); }
```

## Чарты (Chart.js настройки)

```js
Chart.defaults.color = '#625F6A';          // цвет меток
Chart.defaults.borderColor = '#E0E0E0';    // цвет сетки
Chart.defaults.font.family = "Inter, 'Segoe UI', system-ui, sans-serif";

// Основная палитра
const COLORS = {
  orange: '#FF7A00', blue:   '#3C65CC', green:  '#3AC436',
  red:    '#D32F2F', amber:  '#F5A623', purple: '#7C3AED',
  ...
};
```

## Layout

```
Хедер: высота 56px, фиксированный, белый, тень-sm
Сайдбар: ширина 224px, белый, граница справа
Контент: margin-left 224px, padding 24px, фон --bg
```

## Принципы

1. **Светлая тема** — никакого dark mode, фон #F4F4F4, карточки белые
2. **Оранжевый — главный акцент** (#FF7A00), синий — вторичный (#3C65CC)
3. **Inter** — единственный шрифт
4. **Мягкие тени** — не резкие, rgba с малой непрозрачностью
5. **Скруглённые углы** — 10px карточки, 6px мелкие элементы
6. **Hover-эффекты** — translateY(-1px) + усиление тени на карточках
