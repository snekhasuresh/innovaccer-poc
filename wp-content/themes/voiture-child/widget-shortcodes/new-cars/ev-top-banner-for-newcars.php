<?php
function display_ev_top_banner_for_newcars($atts)
{
    // Extract attributes from the shortcode
    $image_id = 327389;
    $image_data = wp_get_attachment_image_src($image_id, 'full'); // 'full' can be replaced with 'thumbnail', 'medium', etc.

    // Validate the image URL
    if (empty($image_data)) {
        return ''; // Return empty if no image data is available
    }

    // Set the image URL
    $image_url = $image_data[0];

    // Output the HTML directly without concatenating or echoing
?>
    <style>
        .home-banner {
            display: block;
            margin: 20px 0;
            border: 2px solid #32D0C6;
            border-radius: 5px;
            overflow: hidden;
        }

        .home-banner img {
            max-width: 100%;
            height: auto;
            /* Maintain aspect ratio */
        }
    </style>
    <a href="<?php echo home_url('/cars-electric'); ?>" class="home-banner">
        <img src="<?php echo esc_url($image_url); ?>" alt="Description of the image" />
    </a>
<?php
}

// Register the shortcode
add_shortcode('ev_top_banner_for_newcars', 'display_ev_top_banner_for_newcars');
