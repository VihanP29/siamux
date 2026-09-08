import asyncio


class ProviderFailure(Exception):
    def __init__(self, code: str, message: str, retryable: bool):
        super().__init__(message)
        self.code = code
        self.retryable = retryable


async def generate(request: dict, attempt: int) -> dict:
    """Deterministic fault injection makes retry experiments reproducible."""
    options = request["mock"]
    await asyncio.sleep(options["delay_seconds"])
    if options["permanent_failure"]:
        raise ProviderFailure("PROVIDER_REJECTED", "Mock provider rejected this request.", False)
    if attempt <= options["fail_first_attempts"]:
        raise ProviderFailure(
            "PROVIDER_UNAVAILABLE", "Mock provider temporarily unavailable.", True
        )
    return {"text": f"Mock response: {request['input']}", "model": request["model"]}


async def invoke(request: dict, attempt: int, timeout: float) -> dict:
    try:
        return await asyncio.wait_for(generate(request, attempt), timeout=timeout)
    except TimeoutError as exc:
        raise ProviderFailure("PROVIDER_TIMEOUT", "Provider exceeded its deadline.", True) from exc
