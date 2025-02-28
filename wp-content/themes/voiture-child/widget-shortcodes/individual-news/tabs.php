<?php
if (!defined('ABSPATH')) {
    exit;
}

function custom_listing_tabs_shortcode($atts)
{
    $atts = shortcode_atts(array(
        'selected_tab' => '',
        'make' => '',
        'model' => '',
        'title' => '',
        'base_url' => '',
    ), $atts);

    global $post;
    $selected_tab = $atts['selected_tab'];
    $overview_tabs = [
        ['tab' => 'Overview', 'url' => $atts['base_url']],
        ['tab' => 'News', 'url' => $atts['base_url'] . '/news'],
        ['tab' => 'Specs', 'url' => $atts['base_url'] . '/specs'],
        ['tab' => 'Gallery', 'url' => $atts['base_url'] . '/gallery']
    ];

    $make = $atts['make'];
    $model_name = $atts['model'];
    $listing_name = $make . '-' . $model_name;
    $make_term = get_term_by('slug', $make, 'listing_make');
    $make_logo_url = get_term_meta($make_term->term_id, 'listing_make_image', true);

    ob_start(); // Start output buffering
?>
    <div class="car-header">
        <span><img src="<?php echo $make_logo_url; ?>" alt="Logo" /></span>
        <span>
            <h1 class="car-title"><?php echo $atts['title'] . ' ' . $selected_tab; ?></h1>
        </span>
    </div>

    <div id="listing-tabs">
        <div class="header-tabs">
            <?php foreach ($overview_tabs as $tab) { ?>
                <div class="inner-container">
                    <a class="header-tab <?php echo $tab['tab'] === $selected_tab ? 'active' : ''; ?>"
                        onclick="changeTab('<?php echo $tab['tab']; ?>')"
                        href="<?php echo $tab['url']; ?>">
                        <?php echo $tab['tab']; ?>
                    </a>
                </div>
            <?php } ?>
        </div>
        <?php do_action('wp-cardealer-single-listing-description', $post); ?>
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
                width: 100px;
                height: 100px;
                border-radius: 50%;
                margin-right: 20px;
            }

            .car-title {
                font-size: 24px;
                font-weight: bold;
                font-style: normal;
            }

            /* Container for the tab headers */
            .inner-container {
                position: relative;
                left: 140px;
            }

            .header-tabs {
                background-color: #0A2357;
                display: flex;
                text-align: center;
                align-items: center;
                padding: 0;
                width: 120%;
                margin: 10px 0;
                height: 60px;
                margin-left: -126px;
            }

            .header-tab {
                padding: 21px 45px;
                cursor: pointer;
                font-size: 18px;
                font-weight: bold;
                color: white;
                text-decoration: none;
                position: relative;
            }

            .header-tab.active {
                color: #3CE9E2;
                background-color: white;
            }

            .header-tab:hover {
                background-color: rgba(255, 255, 255, .15);
                color: white;
            }

            .header-tab.active:hover {
                background-color: white;
                color: #3CE9E2;
            }

            .header-tab.active::after {
                content: '';
                position: absolute;
                left: 0;
                top: 0;
                width: 100%;
                height: 4px;
                background-color: #3CE9E2;
            }
        </style>
    </div>
<?php
    return ob_get_clean();
}

add_shortcode('listing_tabs', 'custom_listing_tabs_shortcode');
