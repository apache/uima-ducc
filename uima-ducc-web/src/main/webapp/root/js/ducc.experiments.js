
function ducc_terminate_experiment(id)
{	
	try {
		$.jGrowl(" Pending termination...");
		$.ajax(
		{
			type: 'POST',
			url : "/ducc-servlet/experiment-cancel-request"+"?id="+id,
			success : function (data) 
			{
			$.jGrowl(data, { life: 6000 });
			setTimeout(function(){window.close();}, 5000);
			}
		});
		setTimeout(function(){window.close();}, 5000);
	}
	catch(err) {
		ducc_error("ducc_terminate_experiment",err);
	}	
	return false;
}

function ducc_confirm_terminate_experiment(id,directory)
{
	try {
		var result=confirm("Terminate experiment "+directory+"?");
		if (result==true) {
  			ducc_terminate_experiment(id);
  		}
  	}
	catch(err) {
		ducc_error("ducc_confirm_terminate_experiment",err);
	}
}
