<?php
// import fuel-consumption.css
function single_listing_fuel_consumption_css()
{
    wp_enqueue_style('fuel-consumption', get_stylesheet_directory_uri() . '/template-listings/single-listing/css/fuel-consumption.css');
}

function overview_fuel_consumption()
{
    if (!defined('ABSPATH')) {
        exit;
    }

    $global_listing_post_data = get_listing_from_query_vars();
    if (empty($global_listing_post_data) || !array($global_listing_post_data)) {
        return;
    }
    $listing_post = $global_listing_post_data['post'];
    $post_title = $listing_post->post_title;
    $variant_posts = $global_listing_post_data['variant_posts'];
    $variants_post_meta = $global_listing_post_data['variant_meta_data'];

    $table_data = array();

    // Loop through all variant posts and collect their metadata
    if (!empty($variant_posts)) {
        foreach ($variant_posts as $variant_post) {
            $variant_meta = $variants_post_meta[$variant_post->ID];
            $fuel_type = isset($variant_meta['fuel_type'][0]) ? $variant_meta['fuel_type'][0] : '';
            $transmission = isset($variant_meta['transmission'][0]) ? $variant_meta['transmission'][0] : '';
            $claim = isset($variant_meta['manufacturers_claim'][0]) ? $variant_meta['manufacturers_claim'][0] : '';

            // Add each variant's data to the table_data array
            if (!empty($fuel_type) && !empty($transmission) && !empty($claim)) {
                $table_data[] = [
                    'fuel_type' => $fuel_type,
                    'transmission' => $transmission,
                    'claim' => $claim
                ];
            }
        }
    }

    if (!empty($table_data)) {
?>
        <div>
            <div class="individula-fuel-title-con">
                <h2 class="individula-fuel-title wa-title-text">Tiêu thụ nhiên liệu <?php echo esc_html($post_title); ?></h2>
            </div>
            <table class="fuel-consumption-table" border="1">
                <thead>
                    <tr>
                        <th>Loại năng lượng</th>
                        <th>Hộp số</th>
                        <th>NSX công bố</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($table_data as $index => $row): ?>
                        <tr class="table-row <?php echo $index >= 3 ? 'hidden-row' : ''; ?>"
                            style="<?php echo $index >= 3 ? 'display: none;' : ''; ?>">
                            <td><?php echo esc_html($row['fuel_type']); ?></td>
                            <td><?php echo esc_html($row['transmission']); ?></td>
                            <td><?php echo esc_html($row['claim']); ?></td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
            <?php if ($table_data): ?>
                <div class="btn-more-container" id="fuel-consumption-view-more-btn" data-url="<?php echo esc_url(home_url('/tieu-hao-nhien-lieu')); ?>">
                    <button class="btn-more">
                        <a href="<?php echo esc_url(parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH) . '/tieu-hao-nhien-lieu'); ?>">
                           Xem thêm
                            <svg width="13" height="13" xmlns="http://www.w3.org/2000/svg"
                                viewBox="0 0 320 512"><!--!Font Awesome Free 6.6.0 by @fontawesome - https://fontawesome.com License - https://fontawesome.com/license/free Copyright 2024 Fonticons, Inc.-->
                                <path
                                    d="M310.6 233.4c12.5 12.5 12.5 32.8 0 45.3l-192 192c-12.5 12.5-32.8 12.5-45.3 0s-12.5-32.8 0-45.3L242.7 256 73.4 86.6c-12.5-12.5 12.5-32.8 0-45.3s32.8-12.5 45.3 0l192 192z" />
                            </svg>
                        </a>
                    </button>
                </div>
            <?php endif; ?>
        </div>

        <script type="text/javascript">
            document.addEventListener('DOMContentLoaded', function() {
                var viewMoreBtn = document.getElementById('view-more-btn');
                var hiddenRows = document.querySelectorAll('.hidden-row');

                if (viewMoreBtn) {
                    viewMoreBtn.addEventListener('click', function() {
                        hiddenRows.forEach(function(row) {
                            row.style.display = 'table-row';
                        });
                        viewMoreBtn.style.display = 'none'; // Hide the button after showing all rows
                    });
                }
            });
        </script>
    <?php
    }
    ?>

    <script>
        document.getElementById('view-more-btn').addEventListener('click', function() {
            const hiddenRows = document.querySelectorAll('.hidden-row');
            hiddenRows.forEach(row => row.style.display = 'table-row');
            this.style.display = 'none';
        });
    </script>
<?php
}
add_shortcode('overview_fuel_consumption', 'overview_fuel_consumption');
