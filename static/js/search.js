$(function() {
    $('#tag-search').submit(function(e) {
        var lang = $('#lang').val();
        if (lang) {
            lang = '?lang=' + lang;
        }
        var tag = $('#search').val();
	if (tag.indexOf('#') == 0) {
	    tag = tag.substring(1, tag.length);
	}
        window.location.href = '/hashtags/search/' + tag + lang;
        e.preventDefault();
    });
});
