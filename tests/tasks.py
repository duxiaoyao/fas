from invoke import task


@task
def test(c, module_or_dir=None, verbose=True, color=True, capture='sys', k=None, x=False, opts='', pty=True):
    """
    Run pytest with given options.

    :param bool verbose:
        Whether to run tests in verbose mode.
    :param bool color:
        Whether to request colorized output (typically only works when
        ``verbose=True``.)
    :param str capture:
        What type of stdout/err capturing pytest should use. Defaults to
        ``sys`` since pytest's own default, ``fd``, tends to trip up
        subprocesses trying to detect PTY status. Can be set to ``no`` for no
        capturing / useful print-debugging / etc.
    :param str module_or_dir:
        Select a specific test module or directory to focus on, e.g. ``main``
        to only run ``tests/main.py``. (Note that this is a specific idiom
        aside from the use of ``-o '-k pattern'``.) Default: ``None``.
    :param str k:
        Convenience passthrough for ``pytest -k``, i.e. test selection.
        Default: ``None``.
    :param bool x:
        Convenience passthrough for ``pytest -x``, i.e. fail-fast. Default:
        ``False``.
    :param str opts:
        Extra runtime options to hand to ``pytest``.
    :param bool pty:
        Whether to use a pty when executing pytest. Default: ``True``.
    """
    flags = []
    if verbose:
        flags.append('--verbose')
    if color:
        flags.append('--color=yes')
    flags.append(f'--capture={capture}')
    if opts:
        flags.append(opts)
    if k is not None and not ('-k' in opts if opts else False):
        flags.append(f"-k '{k}'")
    if x and not ('-x' in opts if opts else False):
        flags.append('-x')
    module_or_dir = '' if module_or_dir is None else f' tests/{module_or_dir}'

    c.run(f"pytest {' '.join(flags)}{module_or_dir}", pty=pty)
