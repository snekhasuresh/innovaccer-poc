jQuery(document).ready(function($) {
    $('#select-cars').on('click', function() {
        $('.dropdown-menu').toggle();
    });

    $('.dropdown-item').on('click', function(e) {
        e.stopPropagation();
        $('#select-cars').text($(this).text());
        $('.dropdown-menu').hide();
    });

    $(document).click(function(e) {
        if (!$(e.target).closest('.car-dropdown-container').length) {
            $('.dropdown-menu').hide();
        }
    });
});