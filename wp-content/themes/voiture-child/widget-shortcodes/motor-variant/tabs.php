<?php

// import css from ./css/tabs.css
function enqueue_motor_tabs_css()
{
    wp_enqueue_style('tabs', get_stylesheet_directory_uri() . '/widget-shortcodes/variant/css/tabs.css');
}

add_shortcode('motor_individual_variant_tabs', 'motor_individual_variant_tabs_shortcode');
function motor_individual_variant_tabs_shortcode($atts)
{
    enqueue_motor_tabs_css();

    // read atts
    $atts = shortcode_atts(array(
        'selected_tab' => 'Tổng quát',
    ), $atts);

    $make = get_query_var('make');
    $model = get_query_var('model');
	$section = get_query_var('section');
	
    $base_url = get_site_url() . '/xe-may/' . $make . '/' . $model;
	
    $make_term = get_term_by('slug', $make, 'motorcycle_make');
    $make_logo_url = get_term_meta($make_term->term_id, 'motorcycle_make_image', true);

    $global_variant_post_data = get_motor_variant_from_query_vars();

    if (!$global_variant_post_data) {
        return;
    }

    $listing_post = $global_variant_post_data['listing_post'];
    $listing_post_title = $listing_post->post_title;
    $variant_post = $global_variant_post_data['variant_post'];
    $variant_post_title = $variant_post->post_title;
    $variant_post_name = $variant_post->post_name;

   $urls = [
        'overview' => $base_url . '/' . $section,
        'news' => $base_url . '/tin-tuc',
        'specs' => $base_url . '/' . $section . '/thong-so-ky-thuat',
        'gallery' => $base_url . '/' . $section . '/hinh-anh'
    ];

    ob_start();

    display_motor_variant_tabs($urls, $atts['selected_tab'], $listing_post_title, $variant_post_title, $make_logo_url);

    return ob_get_clean();
}

function display_motor_variant_tabs($urls, $selected_tab, $listing_name, $variant_name, $make_logo_url)
{
    $overview_tabs = [
        ['tab' => 'Tổng quát', 'url' => $urls['overview']],
        ['tab' => 'Tin tức', 'url' => $urls['news']],
        ['tab' => 'Thông số kỹ thuật', 'url' => $urls['specs']],
        ['tab' => 'Hình ảnh', 'url' => $urls['gallery']]
    ];

    $make = get_query_var('make') ? get_query_var('make') : '';

    switch ($selected_tab) {
        case 'Tin tức':
            $title = $make ? 'Tin tức Xe Ô ' . $listing_name . ' Tô tại Việt Nam ' : $listing_name;
            break;
        case 'Thông số kỹ thuật':
            $title = 'Thông số ' . $variant_name;
            break;
        case 'Hình ảnh':
            $title = 'Hình ảnh & Màu sắc về ' . $variant_name;
            break;
        default:
            $title = $variant_name;
            break;
    }
?>
      <div id="listing-tabs" class="listing-tabs">
  		<span class="car-header container p-l-0">
			<span><img src="<?php echo $make_logo_url; ?>" alt="Logo" /></span>
			<span>
				<h1 class="tab-car-title"><?php echo esc_html($title); ?></h1>
			</span>
		</span>
		<section class="header-tabs">
			<ul class="container header-tabs-container">	
				<?php foreach ($overview_tabs as $tab) { ?>
					<li class="">
						<a class="header-tab <?php echo $tab['tab'] === $selected_tab ? 'active' : ''; ?>"
						   onclick="changeTab('<?php echo $tab['url']; ?>')"
						   href="<?php echo $tab['url']; ?>">
							<?php echo $tab['tab']; ?>
						</a>
					</li>
				<?php } ?>
				</ul>
			
		</section>

        <script>
            function changeTab(tab) {
                // Get all tabs
                const tabs = document.querySelectorAll('.header-tab');

                // Remove active class from all tabs
                tabs.forEach(function(tabElement) {
                    tabElement.classList.remove('active');
                });

                // Add active class to the clicked tab
                event.target.classList.add('active');

                // Update the selected_tab variable
                document.querySelectorAll('.header-tab').forEach(function(tabElement) {
                    if (tabElement.textContent.trim() === tab) {
                        tabElement.classList.add('active');
                    } else {
                        tabElement.classList.remove('active');
                    }
                });

            }
        </script>
    </div>

<?php
}
