import logging
import azure.functions as func
import azure.durable_functions as df


async def main(
	req: func.HttpRequest,
    starter: str
):
    
    client = df.DurableOrchestrationClient(starter)

    options = {}
    for f in [  
                'sqlQuery',
                'sport',
                'eventName',
                'endpointURL',
                'iteration'
                ]:
        if req.params.get(f):
            options[f] = req.params.get(f)

    logging.info("starter----------------> %s",options)

    instance_id = await client.start_new(
		orchestration_function_name="Orchestrator",
		instance_id=None,
		client_input=options
	)

    return func.HttpResponse("It is done?")