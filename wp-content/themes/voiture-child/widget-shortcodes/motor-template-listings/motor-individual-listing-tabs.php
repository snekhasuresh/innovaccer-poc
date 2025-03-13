<?php
function enqueue_listing_bike_tabs_css()
{
    wp_enqueue_style('listing-tabs-motor-style', get_stylesheet_directory_uri() . '/widget-shortcodes/motor-template-listings/css/individual-listing-tabs.css', array(), '1.0', 'all');
}

add_shortcode('individual_listing_bike_tabs', 'individual_listing_motor_tabs_shortcode');
function individual_listing_motor_tabs_shortcode($atts)
{
    enqueue_listing_bike_tabs_css();
    // read atts
    $atts = shortcode_atts(array(
        'make' => '',
        'model' => '',
        'section' => '',
        'variant_section' => '',
        'selected_tab' => 'Tổng quát',
    ), $atts);

    $make = get_query_var('make') ? get_query_var('make') : $atts['make'];
    $model_name = get_query_var('model') ? get_query_var('model') : $atts['model'];
    $section = get_query_var('section') ? get_query_var('section') : $atts['section'];
    $variant_section = get_query_var('variant_section') ? get_query_var('variant_section') : $atts['variant_section'];
    $listing_name = $make . '-' . $model_name;
    $make_term = get_term_by('slug', $make, 'motorcycle_make');
    $make_logo_url = get_term_meta($make_term->term_id, 'motorcycle_make_image', true);

    // get listing post by post name
    $listing_post_query = new WP_Query(array(
        'name' => $listing_name,
        'post_type' => 'motorcycle-listing',
        'posts_per_page' => 1
    ));

    // if no listing post found, return
    if (!$listing_post_query->have_posts()) {
        return;
    }
    $listing_post = $listing_post_query->post;

    if (!empty($variant_section)) {
        $base_url = get_site_url() . '/xe-may/' . $make . '/' . $model_name . '/' . $section;
    } else {
        $base_url = get_site_url() . '/xe-may/' . $make . '/' . $model_name;
    }

 $urls = [
        'tổng quát' => $base_url,
        'tin tức' => $base_url . '/tin-tuc',
        'thông số kỹ thuật' => $base_url . '/thong-so-ky-thuat',
        'hình ảnh' => $base_url . '/hinh-anh'
    ];


    ob_start();

    display_motor_tabs($urls, $atts['selected_tab'], $listing_post->post_title, $make_logo_url);

    return ob_get_clean();
}

function display_motor_tabs($urls, $selected_tab, $listing_name, $make_logo_url)
{
    enqueue_listing_bike_tabs_css();
    $overview_tabs = [
        ['tab' => 'Tổng quát', 'url' => $urls['tong-quat']],
        ['tab' => 'Tin tức', 'url' => $urls['tin-tuc']],
        ['tab' => 'Thông số kỹ thuật', 'url' => $urls['thong-so-ky-thuat']],
        ['tab' => 'Hình ảnh', 'url' => $urls['hinh-anh']]
    ];
    $make = get_query_var('make') ? get_query_var('make') : '';

   switch ($selected_tab) {
        case 'Tin tức':
            $title = $make ? 'Berita Mobil ' . $listing_name . ' di Indonesia ' : $listing_name;
            break;
        case 'Thông số kỹ thuật':
            $title = $selected_tab . ' ' . $listing_name;
            break;
        case 'Hình ảnh':
            $title = 'Gambar Interior & Eksterior ' . $listing_name;
            break;
        default:
            $title = $listing_name;
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
	 <style>
	      .car-header {
                display: flex;
                justify-content: left;
                align-items: center;
            }

            .car-header img {
                width: 53px;
                height: 53px;
                border-radius: 50%;
                margin-right: 20px;
                margin-left: 10px;
            }

            .tab-car-title {
                font-size: 32px;
                line-height: 38px;
                font-family: "Roboto Condensed";
                color: #262626;
                font-weight: 700;
            }
			
			ul {
				list-style-type: none; /* Removes default bullets */
				padding: 0;
			}

			li {
				display: inline; /* Makes the list items appear in a single line */
				margin-right: 10px; /* Optional: Adds space between items */
			}
            /* Container for the tab headers */
            .inner-container {
                position: relative;
                left: 140px;
            }

            .header-tabs {
                background-color: #2e2e2e;
                display: flex;
                align-items: center;
                padding: 0;
                width: 100%;
              	margin: 0px;
                height: 64px;
            }
			
			.p-l-0{
				padding-left: 0px;
			}

            .header-tab {
                padding: 21px 45px;
				top: 6px;
                cursor: pointer;
                font-size: 20px;
                font-weight: bold;
                color: white;
                text-decoration: none;
                position: relative;
                font-family: 'Roboto';
				margin-left:13px;
            }

            .header-tab.active {
                color: #ffb400;
                background-color: white;
            }

            .header-tab:hover {
                background-color: rgba(255, 255, 255, .15);
                color: white;
            }

            .header-tab.active:hover {
                background-color: white;
                color: #ffb400;
            }

            .header-tab.active::after {
                content: '';
                position: absolute;
                left: 0;
                top: 0;
                width: 100%;
                height: 4px;
                background-color: #ffb400;
            }
			@media screen and (max-width: 768px) {
				 .header-tab {
					padding: 21px 45px;
					cursor: pointer;
					font-size: 20px;
					font-weight: bold;
					color: white;
					text-decoration: none;
					position: relative;
					font-family: 'Roboto';
					white-space: nowrap;
				}
				.header-tabs {
					background-color: #2e2e2e;
					display: flex;
					text-align: center;
					align-items: center;
					padding: 0;
					width: 100%;
					overflow-x: scroll;
					margin: 10px 0;
					height: 64px;
					scrollbar-width: none;
				}
				.header-tabs-container{
					display: flex !important;
					justify-content:flex-start;
				}
				.tab-car-title {
					font-size: 32px;
					line-height: 38px;
					font-family: "Roboto Condensed";
					color: #262626;
					font-weight: 700;
					margin-left: 33px;
				}
			}
	 </style>
    </div>

<?php
}
