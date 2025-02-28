jQuery(document).ready(function($){
    var mediaUploader;

    $('.upload_image_button').click(function(e) {
        e.preventDefault();

        // If the uploader object has already been created, reopen the dialog
        if (mediaUploader) {
            mediaUploader.open();
            return;
        }

        // Extend the wp.media object
        mediaUploader = wp.media.frames.file_frame = wp.media({
            title: 'Choose Image',
            button: {
                text: 'Choose Image'
            },
            multiple: false
        });

        // When an image is selected, run a callback
        mediaUploader.on('select', function() {
            var attachment = mediaUploader.state().get('selection').first().toJSON();
            $('#listing_make_image').val(attachment.url);
            $('#listing_make_image_preview img').attr('src', attachment.url);
        });

        // Open the uploader dialog
        mediaUploader.open();
    });
});
