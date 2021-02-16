import logging
import azure.functions as func
import azure.durable_functions as df


async def main(
	req: func.HttpRequest,
    starter: str
):
    
    client = df.DurableOrchestrationClient(starter)

    options = {}
    necessary = [
        'query',
        'sport',
        'event'
    ]
    bonus = [
        # 'endpointURL',
        # 'iteration'
    ]
    for f in necessary + bonus:
        _val_ =  req.params.get(f)
        options[f] = _val_
        if (f in necessary) & (_val_ is None):
            raise ValueError(f"`{f}` is required, but not provided")

    logging.info("starter----------------> %s",options)

    instance_id = await client.start_new(
		orchestration_function_name="Orchestrator",
		instance_id=None,
		client_input=options
	)

    return client.create_check_status_response(req, instance_id)