# routing_engine/application/holiday_utils.py

from datetime import date, timedelta


def _easter(year: int) -> date:
    """Butcher's algorithm for Easter Sunday."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def feriados_nacionais(year: int) -> set[date]:
    """
    Returns the set of Brazilian national holidays for a given year.
    Includes fixed holidays and moveable holidays based on Easter.
    Carnaval Mon/Tue are included as they are widely observed.
    """
    easter = _easter(year)

    fixed = {
        date(year, 1, 1),   # Confraternização Universal
        date(year, 4, 21),  # Tiradentes
        date(year, 5, 1),   # Dia do Trabalho
        date(year, 9, 7),   # Independência
        date(year, 10, 12), # N. Sra. Aparecida
        date(year, 11, 2),  # Finados
        date(year, 11, 15), # Proclamação da República
        date(year, 11, 20), # Consciência Negra (Lei 14.759/2023)
        date(year, 12, 25), # Natal
    }

    moveable = {
        easter - timedelta(days=48),  # Carnaval Segunda
        easter - timedelta(days=47),  # Carnaval Terça
        easter - timedelta(days=2),   # Sexta-feira Santa
    }

    return fixed | moveable


def dias_uteis(inicio: date, fim: date) -> list[date]:
    """
    Returns ordered list of working days (Mon-Fri, excluding national holidays)
    between inicio and fim inclusive.
    """
    holidays: set[date] = set()
    for year in range(inicio.year, fim.year + 1):
        holidays |= feriados_nacionais(year)

    result = []
    current = inicio
    while current <= fim:
        if current.weekday() < 5 and current not in holidays:
            result.append(current)
        current += timedelta(days=1)
    return result
