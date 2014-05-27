<script>
window.onload=init;
function init() {
	hide('q2');
	hide('q3');
	show('q1');
}
function hide(id) {
	//alert('hide:'+id);
	element = document.getElementById(id)
	element.style.display='none';
}
function show(id) {
	//alert('show:'+id);
	element = document.getElementById(id)
	element.style.display='';
}
function goLeft(location) {
	if(location == 'q1') {
		hide('q1');
		hide('q2');
		show('q3');
	}
	else if(location == 'q2') {
		hide('q2');
		hide('q3');
		show('q1');
	}
	else if(location == 'q3') {
		hide('q3');
		hide('q1');
		show('q2');
	}
}
function goRight(location) {
	if(location == 'q1') {
		hide('q3');
		hide('q1');
		show('q2');
	}
	else if(location == 'q2') {
		hide('q1');
		hide('q2');
		show('q3');
	}
	else if(location == 'q3') {
		hide('q3');
		hide('q2');
		show('q1');
	}
}
</script>