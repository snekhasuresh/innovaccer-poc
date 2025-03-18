<?php

// import css from ./css/tabs.css
function enqueue_tabs_css()
{
    wp_enqueue_style('tabs', get_stylesheet_directory_uri() . '/widget-shortcodes/variant/css/tabs.css');
}

add_shortcode('individual_variant_tabs', 'individual_variant_tabs_shortcode');
function individual_variant_tabs_shortcode($atts)
{
    enqueue_tabs_css();

    // read atts
    $atts = shortcode_atts(array(
        'selected_tab' => 'Tổng quát',
    ), $atts);

    $make = get_query_var('make');
    $model = get_query_var('model');
	$section = get_query_var('section');
	
    $base_url = get_site_url() . '/xe-oto/' . $make . '/' . $model;

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
        'tổng quát' => $base_url . '/' . $section,
        'tin tức' => $base_url . '/tin-tuc',
        'thông số kỹ thuật' => $base_url . '/' . $section . '/thong-so-ky-thuat',
        'hình ảnh' => $base_url . '/' . $section . '/hinh-anh'
    ];

    ob_start();

    display_variant_tabs($urls, $atts['selected_tab'], $listing_post_title, $variant_post_title, $make_logo_url);

    return ob_get_clean();
}

function display_variant_tabs($urls, $selected_tab, $listing_name, $variant_name, $make_logo_url)
{
    $overview_tabs = [
        ['tab' => 'Tổng quát', 'url' => $urls['tổng quát']],
        ['tab' => 'Tin tức', 'url' => $urls['tin tức']],
        ['tab' => 'Thông số kỹ thuật', 'url' => $urls['thông số kỹ thuật']],
        ['tab' => 'Hình ảnh', 'url' => $urls['hình ảnh']]
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
            $title = 'Hình ảnh nội thất và ngoại thất ' . $listing_name;
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
					display: flex;
					
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
