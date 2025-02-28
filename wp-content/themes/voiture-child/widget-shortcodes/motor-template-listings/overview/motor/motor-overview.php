<?php
function enqueue_motor_overview_css()
{
    wp_enqueue_style('bike-overview-style', get_stylesheet_directory_uri() . '/widget-shortcodes/motor-template-listings/overview/motor/css/car-overview.css');
}

function single_listing_motor_overview_shortcode()
{
    enqueue_motor_overview_css();

    $global_listing_post_data = get_motor_listing_from_query_vars();
    if (empty($global_listing_post_data) || !array($global_listing_post_data)) {
        return;
    }

    $post = $global_listing_post_data['post'];

    $post_id = $post->ID;
    $post_title = $post->post_title;

    // Retrieve all overview rows
    $overview_rows = get_field('overview', $post_id);

    if (empty($overview_rows) || !is_array($overview_rows)) {
        return;
    }

    // Filter rows with valid descriptions
    $filtered_rows = array_filter($overview_rows, function ($row) {
        return !empty($row['overview']);
    });

    if (empty($filtered_rows)) {
        return;
    }

?>
    <div>
        <div class="individual-review-title-con">
            <h2 class="individual-review-title wa-title-text">รีวิว <?php echo esc_html($post_title); ?></h2>
        </div>
        <div class="dimension-tabs">
            <div class="tabs-left">
                <ul class="dimension-list">
                    <?php foreach ($filtered_rows as $index => $row): ?>
                        <li class="tab-item <?php if ($index === 0) echo 'active'; ?>" data-tab="tab-<?php echo $index; ?>">
                            <?php echo esc_html($row['select_dimension']); ?>
                        </li>
                    <?php endforeach; ?>
                </ul>
            </div>
            <div class="tabs-right">
                <?php foreach ($filtered_rows as $index => $row): ?>
                    <div class="tab-content <?php if ($index === 0) echo 'active'; ?>" id="tab-<?php echo $index; ?>">
                        <div class="dimension-cont">
                            <h3 class="dimension-title"><?php echo esc_html($row['select_dimension']); ?></h3>
                        </div>
                        <?php echo $row['overview']; ?>
                    </div>
                <?php endforeach; ?>
            </div>
            <script>
                jQuery(document).ready(function($) {
                    $('.tab-item').on('click', function() {
                        var tabId = $(this).data('tab');

                        $('.tab-item').removeClass('active');
                        $('.tab-content').removeClass('active').hide();

                        $(this).addClass('active');
                        $('#' + tabId).fadeIn(400).addClass('active');
                    });
                });
            </script>
        </div>
    </div>
<?php
}
add_shortcode('single_listing_motor_overview', 'single_listing_motor_overview_shortcode');
