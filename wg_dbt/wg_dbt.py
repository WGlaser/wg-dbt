"""Flow for running any dbt project."""

import shutil

from prefect import flow, get_run_logger
from prefect_dbt import DbtCliProfile, DbtCoreOperation
from prefect_github import GitHubRepository


@flow(flow_run_name='wg_dbt-{api_name}')
def wg_dbt(
    api_name: str,
    model: str = None,
    run: bool = True,
) -> None:
    """Runs a dbt project for a given api.

    Args:
        api_name (str): The name of the api.
        model (str, optional): Path to a specific model that needs to run.
        source_freshness (bool, optional):
        run (bool): Whether to run a specific model or all models
        test (bool, optional): Whether to run `dbt test`
    Raises:
        e: RuntimeError
    """
    logger = get_run_logger()
    logger.info("trying to load blocks")
    github_repository: GitHubRepository = GitHubRepository.load(f'dbt-{api_name}')

    github_repository.get_directory(local_path='dbt')

    dbt_cli_profile: DbtCliProfile = DbtCliProfile.load(api_name)

    logger.info("loaded github and dbt cli profile blocks")
    deps_result = DbtCoreOperation(
        commands=['dbt deps'],
        overwrite_profiles=True,
        dbt_cli_profile=dbt_cli_profile,
        project_dir='dbt',
        stream_output=False,
    ).run()
    logger.info('\n'.join(deps_result))

    if run:
        if model is None:
            command = ['dbt run']
        else:
            command = [f'dbt run --select {model}']

        run_result = DbtCoreOperation(  # noqa: F841
            commands=command,
            overwrite_profiles=True,
            dbt_cli_profile=dbt_cli_profile,
            project_dir='dbt',
            stream_output=True,
        ).run()

    shutil.rmtree('dbt')
    shutil.rmtree('dbt_packages')


if __name__ == '__main__':
    wg_dbt(api_name='seatgeek', model='seatgeek')
