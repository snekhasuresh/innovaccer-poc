<?php
if (!defined('ABSPATH')) {
    exit;
}
global $post;
global $selected_tab;
$selected_tab = 'Overview';
$overview_tabs = ['Overview', 'News', 'Specs', 'Gallery'];

// get post make
$listing_make = get_post_meta($post->ID, '_listing_make', true);
// get term name of term id = $listing_make
$listing_make = get_term_by('id', $listing_make, 'listing_make')->slug;

$name_array = explode("-", $post->post_name);
// remove first element and append other with hyphen
$model = implode("-", array_slice($name_array, 1));
// construct listing title format: http://wapcar.my.localdev/listing/proton-x70/
$listing_url = get_site_url() . '/listing/' . $atts['make'] . '-' . $atts['model'] . '/';
$base_url = get_site_url() . '/cars/' . $listing_make . '/' . $model;

$selected_tab = 'Overview';
$overview_tabs = [
    ['tab' => 'Overview', 'url' => $listing_url],
    ['tab' => 'News', 'url' => $base_url . '/news'],
    ['tab' => 'Specs', 'url' => $base_url . '/specs'],
    // ['tab' => 'Gallery', 'url' => $base_url . '/gallery']
];
?>

<div id="listing-tabs">

    <div class="header-tabs">
        <?php foreach ($overview_tabs as $tab) { ?>
            <div class="inner-container">
                <a class="header-tab <?php echo $tab['tab'] === $selected_tab ? 'active' : ''; ?>"
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

            console.log(selected_tab);
        }
    </script>

    <style>
        /* Container for the tab headers */
        .inner-container {
            position: relative;
            left: 120px;
        }

        .header-tabs {
            background-color: #0B0E52;
            display: flex;
            text-align: center;
            gap: 40px;
            padding: 0;
            /* Remove padding to eliminate gaps */

            width: 118%;
            position: relative;
            left: -115px;
            margin: 10px 0px;
            /* Ensure no margin between tabs and content */
        }

        /* Individual tab */
        .header-tab {
            padding: 13px 22px;
            /* Adjust padding to create a clean alignment */
            cursor: pointer;
            font-size: 16px;
            font-weight: bold;
            color: white;
            position: relative;
            transition: color 0.3s ease;
        }

        /* Active tab with background reaching the top edge */
        .header-tab.active {
            color: #32D0C6;
            background-color: white;

            /* Rounded corners only at the top */
            margin-bottom: -3px;
            /* Align the bottom edge perfectly with the border */
        }

        /* Hover effect */


        /* Underline for the active and hover tab on top */
        .header-tab.active::after {
            content: '';
            position: absolute;
            left: 0;
            top: 0;
            width: 100%;
            height: 3px;
            background-color: #32D0C6;
            transition: width 0.3s ease;
        }

        .header-tab:hover::after {
            content: '';
            position: absolute;
            left: 0;
            top: 0;
            width: 100%;
            /* height: 3px; */
            background-color: #32D0C6;
            transition: width 0.3s ease;
        }
    </style>
</div>