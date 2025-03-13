<?php
function enqueue_listing_tabs_css()
{
    wp_enqueue_style('listing-tabs-style', get_stylesheet_directory_uri() . '/widget-shortcodes/common/css/individual-listing-tabs.css', array(), '1.0', 'all');
}

add_shortcode('individual_listing_tabs', 'individual_listing_tabs_shortcode');
function individual_listing_tabs_shortcode($atts)
{
    enqueue_listing_tabs_css();
    // read atts
    $atts = shortcode_atts(array(
        'make' => '',
        'model' => '',
        'section' => '',
        'variant_section' => '',
        'selected_tab' => 'tong-quat',
    ), $atts);

    $make = get_query_var('make') ? get_query_var('make') : $atts['make'];
    $model_name = get_query_var('model') ? get_query_var('model') : $atts['model'];
    $section = get_query_var('section') ? get_query_var('section') : $atts['section'];
    $variant_section = get_query_var('variant_section') ? get_query_var('variant_section') : $atts['variant_section'];
    $listing_name = $make . '-' . $model_name;
    $make_term = get_term_by('slug', $make, 'listing_make');
    $make_logo_url = get_term_meta($make_term->term_id, 'listing_make_image', true);

    // get listing post by post name
    $listing_post_query = new WP_Query(array(
        'name' => $listing_name,
        'post_type' => 'listing',
        'posts_per_page' => 1
    ));

    // if no listing post found, return
    if (!$listing_post_query->have_posts()) {
        return;
    }
    $listing_post = $listing_post_query->post;

    if (!empty($variant_section)) {
        $base_url = get_site_url() . '/xe-oto/' . $make . '/' . $model_name . '/' . $section;
    } else {
        $base_url = get_site_url() . '/xe-oto/' . $make . '/' . $model_name;
    }

    $urls = [
        'tong-quat' => $base_url,
        'tin-tuc' => $base_url . '/tin-tuc',
        'thong-so-ky-thuat' => $base_url . '/thong-so-ky-thuat',
        'hinh-anh' => $base_url . '/hinh-anh'
    ];

    ob_start();

    display_tabs($urls, $atts['selected_tab'], $listing_post->post_title, $make_logo_url);

    return ob_get_clean();
}

function display_tabs($urls, $selected_tab, $listing_name, $make_logo_url)
{
    enqueue_listing_tabs_css();
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

    <div class="car-header">
        <span><img src="<?php echo $make_logo_url; ?>" alt="Logo" /></span>
        <span>
            <h1 class="tab-car-title"><?php echo esc_html($title); ?></h1>
        </span>
    </div>

    <div id="listing-tabs">
        <div class="header-tabs">
            <?php foreach ($overview_tabs as $tab) { ?>
                <div class="inner-container">
                    <a class="header-tab <?php echo $tab['tab'] === $selected_tab ? 'active' : ''; ?>"
                        onclick="changeTab('<?php echo $tab['url']; ?>')"
                        href="<?php echo $tab['url']; ?>">
                        <?php echo $tab['tab']; ?>
                    </a>
                </div>
            <?php } ?>
        </div>

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
