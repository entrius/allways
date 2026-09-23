"""Shared wizard chrome for `alw`: the startup banner, numbered steps, and result panels.

Pure presentation — no chain or config access — so every wizard-style command renders the same way.
"""

from __future__ import annotations

from typing import Iterable, Sequence

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

BRAND = 'cyan'

# figlet "larry3d"; rendered once so the CLI carries no figlet dependency.
BANNER = r"""
 ______  __       __       __      __  ______   __    __  ____
/\  _  \/\ \     /\ \     /\ \  __/\ \/\  _  \ /\ \  /\ \/\  _`\
\ \ \L\ \ \ \    \ \ \    \ \ \/\ \ \ \ \ \L\ \\ `\`\\/'/\ \,\L\_\
 \ \  __ \ \ \  __\ \ \  __\ \ \ \ \ \ \ \  __ \`\ `\ /'  \/_\__ \
  \ \ \/\ \ \ \L\ \\ \ \L\ \\ \ \_/ \_\ \ \ \/\ \ `\ \ \    /\ \L\ \
   \ \_\ \_\ \____/ \ \____/ \ `\___x___/\ \_\ \_\  \ \_\   \ `\____\
    \/_/\/_/\/___/   \/___/   '\/__//__/  \/_/\/_/   \/_/    \/_____/
"""


def draw_logo(console: Console, tagline: str | None = None) -> None:
    """Print the ALLWAYS banner in the brand colour, optionally followed by a bold tagline."""
    console.print(f'[bold {BRAND}]{BANNER}[/bold {BRAND}]', highlight=False, markup=True)
    if tagline:
        console.print(f'[bold {BRAND}]{tagline}[/bold {BRAND}]\n')


def draw_step(console: Console, number: int, title: str, *notes: str) -> None:
    """A numbered step header plus dim explanatory lines beneath it."""
    console.print(f'\n[bold {BRAND}]Step {number}:[/bold {BRAND}] [bold]{title}[/bold]')
    for note in notes:
        console.print(f'  [dim]{note}[/dim]')


def draw_done(console: Console, message: str) -> None:
    """A green check line for a step that completed or was already satisfied."""
    console.print(f'  [green]✓[/green] {message}')


def draw_skip(console: Console, message: str) -> None:
    """A dim dash line for a step the operator chose to skip."""
    console.print(f'  [dim]– {message}[/dim]')


def draw_warn(console: Console, message: str) -> None:
    console.print(f'  [yellow]![/yellow] {message}')


def draw_kv(console: Console, rows: Iterable[tuple[str, str]], key_style: str = f'bold {BRAND}') -> None:
    """Aligned key/value lines, indented under the current step."""
    rows = list(rows)
    if not rows:
        return
    width = max(len(k) for k, _ in rows)
    for key, value in rows:
        console.print(f'  [{key_style}]{key.ljust(width)}[/{key_style}]  {value}', highlight=False)


def draw_panel(console: Console, lines: Sequence[str], title: str, border: str = BRAND) -> None:
    console.print(Panel('\n'.join(lines), title=f'[bold]{title}[/bold]', border_style=border, box=box.ROUNDED))


def draw_success_box(console: Console, lines: Sequence[str], title: str = 'Success') -> None:
    draw_panel(console, lines, title, border='green')


def draw_next_steps(console: Console, rows: Iterable[tuple[str, str]]) -> None:
    """A 'Next steps' block: command on the left, what it does on the right."""
    console.print(f'\n[bold {BRAND}]Next steps:[/bold {BRAND}]')
    draw_kv(console, rows, key_style='bold')
    console.print()


def check_table(rows: Iterable[tuple[bool | None, str, str]]) -> Table:
    """Preflight table: (ok, check, detail) → ✓ / ✗ / – rows. ``None`` = not applicable."""
    table = Table(show_header=True, box=box.SIMPLE, header_style='bold')
    table.add_column('', width=2, justify='center')
    table.add_column('Check', style='bold', no_wrap=True)
    table.add_column('Detail', overflow='fold')
    for ok, check, detail in rows:
        mark = '[green]✓[/green]' if ok else '[dim]–[/dim]' if ok is None else '[red]✗[/red]'
        table.add_row(mark, check, detail)
    return table
