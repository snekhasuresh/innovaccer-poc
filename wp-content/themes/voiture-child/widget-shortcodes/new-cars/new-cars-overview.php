<?php
function newcars_overview($atts)
{
    // Extract attributes from the shortcode
    $atts = shortcode_atts(array(
        'image_url' => esc_url(get_site_url() . '/wp-content/uploads/2024/10/display_ev_top_banner_for_newcars.jpg'),  // Image URL
        'link_url' => '',   // Link URL
        'alt_text' => '',   // Alt text for the image
    ), $atts);

    // Validate the image URL
    if (empty($atts['image_url'])) {
        return ''; // Return empty if no image URL is provided
    }

    // Output the HTML directly without concatenating or echoing
?>
    <div class="new-cars-overview">
        <span>Latest information about all new and upcoming cars in Indonesia. We provide car news, car specifications, features, prices, pictures, videos, and expert and owner reviews. You can search for your ideal car based on brand, budget, body type, fuel type, transmission, seats, segment, driveline, and features you want to check first.</span>
    </div>
    <style>
        .new-cars-overview {
            background-color: #F9F9F9;
            padding: 15px;
            margin-top: -10px;
			margin-left:30px;
        }
		.new-cars-overview span{
			font-size:14px;
			font-family:'Roboto';
			color:#262626;
			font-weight:400
		}
		@media screen and (max-width: 768px) {
			   .new-cars-overview {
            background-color: #F9F9F9;
            padding: 15px;
            margin-top: -10px;
			margin-left:0px !important;
        }
		}
    </style>

<?php
}

add_shortcode('newcars_overview', 'newcars_overview');
