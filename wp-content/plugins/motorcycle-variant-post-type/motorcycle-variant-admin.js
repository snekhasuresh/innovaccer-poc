jQuery(document).ready(function($){
    $('#upload_motorcycle_variant_image_button').click(function(e) {
        e.preventDefault();

        var image_frame;
        if (image_frame) {
            image_frame.open();
        }

        image_frame = wp.media({
            title: 'Select or Upload Image',
            button: {
                text: 'Use this image',
            },
            multiple: false
        });

        image_frame.on('select', function() {
            var attachment = image_frame.state().get('selection').first().toJSON();
            $('#motorcycle_variant_image').val(attachment.url);
        });

        image_frame.open();
    });
});
