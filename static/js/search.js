$(function() {
    $('#tag-search').submit(function(e) {
        var lang = $('#lang').val();
        if (lang) {
            lang = '?lang=' + lang;
        }
        var tag = $('#search').val();
        window.location.href = '/hashtags/search/' + tag + lang;
        e.preventDefault();
    });
});
