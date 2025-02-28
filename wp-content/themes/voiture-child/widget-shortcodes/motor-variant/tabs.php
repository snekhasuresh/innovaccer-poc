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
        'selected_tab' => 'Overview',
    ), $atts);

    $make = get_query_var('make');
    $model = get_query_var('model');
    $base_url = get_site_url() . '/cars/' . $make . '/' . $model;

    $make_term = get_term_by('slug', $make, 'listing_make');
    $make_logo_url = get_term_meta($make_term->term_id, 'listing_make_image', true);

    $global_variant_post_data = get_variant_from_query_vars();

    if (!$global_variant_post_data) {
        return;
    }

    $listing_post = $global_variant_post_data['listing_post'];
    $listing_post_title = $listing_post->post_title;
    $variant_post = $global_variant_post_data['variant_post'];
    $variant_post_title = $variant_post->post_title;
    $variant_post_name = $variant_post->post_name;

    $urls = [
        'overview' => $base_url . '/' . $variant_post_name,
        'news' => $base_url . '/news',
        'specs' => $base_url . '/' . $variant_post_name . '/specs',
        'gallery' => $base_url . '/' . $variant_post_name . '/gallery'
    ];

    ob_start();

    display_motor_variant_tabs($urls, $atts['selected_tab'], $listing_post_title, $variant_post_title, $make_logo_url);

    return ob_get_clean();
}

function display_motor_variant_tabs($urls, $selected_tab, $listing_name, $variant_name, $make_logo_url)
{
    $overview_tabs = [
        ['tab' => 'ภาพรวม', 'url' => $urls['overview']],
        ['tab' => 'ข่าวสาร', 'url' => $urls['news']],
        ['tab' => 'สเปค', 'url' => $urls['specs']],
        ['tab' => 'รูปภาพ', 'url' => $urls['gallery']]
    ];

    $make = get_query_var('make') ? get_query_var('make') : '';

    switch ($selected_tab) {
        case 'News':
            $title = $make ? $listing_name . ' ' . $selected_tab . ' in Malaysia' : $listing_name;
            break;
        case 'Specs':
            $title = $variant_name . ' ' . $selected_tab;
            break;
        case 'Gallery':
            $title = $variant_name . ' Interior & Exterior Images';
            break;
        default:
            $title = $variant_name;
            break;
    }
?>

    <div class="car-header">
        <span><img src="<?php echo $make_logo_url; ?>" alt="Logo" /></span>
        <span>
            <h1 class="tab-car-title"><?php echo esc_html($title); ?></h1>
        </span>
    </div>

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
