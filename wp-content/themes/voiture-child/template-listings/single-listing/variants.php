<?php
function enqueue_overview_variants_css()
{
    wp_enqueue_style('overview-variants-style', get_stylesheet_directory_uri() . '/template-listings/single-listing/css/variants.css', array(), '1.0', 'all');
}
// add_action('wp_enqueue_scripts', 'enqueue_overview_variants_css');

function single_listing_variants()
{
$translate = [
    
'News' => 'Tin tức',

];


    enqueue_overview_variants_css();

    $global_listing_post_data = get_listing_from_query_vars();
    $listing_post = $global_listing_post_data['post'];
    $post_title = $listing_post->post_title;
    $variant_posts = $global_listing_post_data['variant_posts'];
    $variants_post_meta = $global_listing_post_data['variant_meta_data'];

    if (empty($variant_posts)) {
        return;
    }

    // group by on_sale and not on_sale, on_sale=Yes, state=1
    $variants_on_sale = [];
    $variants_not_on_sale = [];

    $highest_price = 0;
    $highest_price_variant_name = '';
    $lowest_price_variant_name = '';
    $lowest_price = 0;
    $variant_on_sale_count = 0;

    foreach ($variant_posts as $variant_post) {
        $variant_id = $variant_post->ID;
        $post_meta = $variants_post_meta[$variant_id];
        $on_sale = $post_meta['on_sale'][0] ?? 'No';
        $state = $post_meta['state'][0] ?? 0;
        $price = $post_meta['retail_price'][0] ?? 0;
        if ($on_sale == 'Yes' && $state == 1) {
            $variant_name = $variant_post->post_title;
            $variant_on_sale_count++;

            if ($price && $price > $highest_price) {
                $highest_price = $price;
                $highest_price_variant_name = $variant_name;
            }
            if ($price && ($price < $lowest_price || $lowest_price == 0)) {
                $lowest_price = $price;
                $lowest_price_variant_name = $variant_name;
            }
        }

        // Group key based on combined values
        if ($state == 1) {
            if ($on_sale == 'Yes') {
                $variants_on_sale[] = $variant_post;
            } else {
                $variants_not_on_sale[] = $variant_post;
            }
        }
    }

    if ($highest_price == $lowest_price && $highest_price != 0) {
        $price = format_price_vietnam($lowest_price);
    } else {
        $price = format_price_vietnam($lowest_price) . ' - ' . format_price_vietnam($highest_price);
    }

    $current_year = date("Y");
    $next_year = $current_year + 1;
    $year_range = $current_year . ' - ' . $next_year;
?>

    <!-- launched_year| engine |aspiration_form -->
    <div class="row">

        <div class="varient-page-container">
            <div id="listing-detail-description" class="description inner col-md-12">
                <h2 class=" wa-title-text"><?php esc_html_e('Bảng giá (mẫu xe) '.$post_title, 'voiture'); ?></h2>

                <div class="tabs-container-varient">
                    <?php if (!empty($variants_on_sale) && count($variants_on_sale) > 1) { ?>
                       <div class="price-dec">
    				 <?php 
                        // Dynamically generate the description
                        echo $year_range . ' ' . $post_title . ' is offered in ' . $variant_on_sale_count . 
                        ' variants - which are priced from ' . format_price_vietnam($lowest_price) . 
                        ' to ' . format_price_vietnam($highest_price) . '. The base model of ' . $post_title . 
                        ' is ' . $lowest_price_variant_name . ' which is at a price of ' . format_price_vietnam($lowest_price) . ' and the top 							variant of ' . $post_title . ' is ' . $highest_price_variant_name . ' which is offered at a price of ' . 
                        format_price_vietnam($highest_price) . '.';
                        ?>
						</div>
                    <?php } ?>
                    <div class="custom-tabs-header">
                        <?php if (!empty($variants_on_sale)) { ?>
                            <button class="custom-tab-btn custom-tab-active" data-tab="tab-on-sale">
                                <?php esc_html_e('Đang bán', 'your-textdomain'); ?>
                            </button>
                        <?php } ?>
                        <?php if (!empty($variants_not_on_sale)) { ?>
                            <button class="custom-tab-btn  <?php echo (!empty($variants_not_on_sale) && empty($variants_on_sale)) ? 'custom-tab-active' : '' ?>" data-tab="tab-not-on-sale">
                                <?php esc_html_e('Not On Sale Variants', 'your-textdomain'); ?>
                            </button>
                        <?php } ?>
                    </div>
                    <?php if (!empty($variants_on_sale)) { ?>
                        <div id="tab-on-sale" class="tab-content <?php echo !empty($variants_on_sale) ? 'active' : '' ?>">
                            <div class="car-list-varient">
                                <?php foreach ($variants_on_sale as $variant) {
                                    $variant_id = $variant->ID;
                                    $meta_data = $variants_post_meta[$variant_id];

                                    $launched_year = isset($meta_data['launched_year'][0]) ? $meta_data['launched_year'][0] : 'N/A';
                                    $engine = isset($meta_data['engine'][0]) ? $meta_data['engine'][0] : 'N/A';
                                    $aspiration_form = isset($meta_data['aspiration_form'][0]) ? $meta_data['aspiration_form'][0] : 'N/A';

                                    $engine_parts = explode(' ', $engine);
                                    $engine_display = $engine_parts[0];
                                    // Group key based on combined values
                                    $group_key = $launched_year . ' | ' . $engine_display . ' | ' . $aspiration_form;
									$price_data = $meta_data['retail_price'][0];
                                    // Add post to the grouped array
                                    $grouped_variants[$group_key][] = [
                                        'title' => $variant->post_title,
                                        'post_name' => $variant->post_name,
                                        'retail_price' => (isset($meta_data['retail_price'][0]) && $meta_data['retail_price'][0] != 0) ? format_price_vietnam($meta_data['retail_price'][0]) : 'Đang cập nhật',
                                        'monthly_payment' => (isset($meta_data['monthly_payment'][0]) && $meta_data['monthly_payment'][0] != 0) ? format_price_vietnam($meta_data['monthly_payment'][0]) : 'Đang cập nhật',
                                    ];
                                }
                                wp_reset_postdata();

                                // Display grouped data
                                foreach ($grouped_variants as $group_key => $variants) {
                                ?>
                                    <div class="header">
                                        <span><?php echo esc_html($group_key); ?></span>
                                        <span>Giá xe</span>
                                    </div>
                                    <?php foreach ($variants as $variant) { ?>
                                        <div class="car-item-c">
                                            <?php
                                            $base_url = get_home_url();
                                            $make = get_query_var('make');
                                            $model = get_query_var('model');
                                            $variant_url = $base_url . '/xe-oto/' . $make . '/' . $model . '/' . $variant['post_name'];
                                            ?>
                                            <div class="car-name"><a href="<?php echo $variant_url; ?>"><?php echo esc_html($variant['title']); ?></a></div>
                                            <div class="right-content">
                                                <div class="price-section">
                                                    <div class="total-price"><?php echo $variant['retail_price']; ?></div>
                                                    <div class="monthly-price">
                                                        <?php echo $variant['monthly_payment'] . '/tháng'; ?>
                                                    </div>
                                                </div>
                                                <div class="calculator-icon">
													<a href="<?php echo home_url('dung-cu/mua-xe-tra-gop'); ?>">
                                                    <img src="data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPHN2ZyB3aWR0aD0iMjJweCIgaGVpZ2h0PSIyMnB4IiB2aWV3Qm94PSIwIDAgMjIgMjIiIHZlcnNpb249IjEuMSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIiB4bWxuczp4bGluaz0iaHR0cDovL3d3dy53My5vcmcvMTk5OS94bGluayI+CiAgICA8dGl0bGU+6K6h566X5ZmoPC90aXRsZT4KICAgIDxnIGlkPSLmjqfku7YiIHN0cm9rZT0ibm9uZSIgc3Ryb2tlLXdpZHRoPSIxIiBmaWxsPSJub25lIiBmaWxsLXJ1bGU9ImV2ZW5vZGQiPgogICAgICAgIDxnIGlkPSJWYXJpYW50c19saXN0IiB0cmFuc2Zvcm09InRyYW5zbGF0ZSgtNTczLjAwMDAwMCwgLTI2LjAwMDAwMCkiIGZpbGwtcnVsZT0ibm9uemVybyI+CiAgICAgICAgICAgIDxnIGlkPSLorqHnrpflmagiIHRyYW5zZm9ybT0idHJhbnNsYXRlKDU3NC4wMDAwMDAsIDI3LjAwMDAwMCkiPgogICAgICAgICAgICAgICAgPHJlY3QgaWQ9IuefqeW9oiIgc3Ryb2tlPSIjQkZCRkJGIiBmaWxsLW9wYWNpdHk9IjAiIGZpbGw9IiNGRkZGRkYiIHg9IjAiIHk9IjAiIHdpZHRoPSIyMCIgaGVpZ2h0PSIyMCIgcng9IjIiPjwvcmVjdD4KICAgICAgICAgICAgICAgIDxnIGlkPSLnvJbnu4QiIGNsYXNzPSJjYWxjIiB0cmFuc2Zvcm09InRyYW5zbGF0ZSg0LjAwMDAwMCwgNC4wMDAwMDApIiBmaWxsPSIjMjYyNjI2Ij4KICAgICAgICAgICAgICAgICAgICA8cGF0aCBkPSJNMC43NzA4ODAwMzIsMS44NzYwNSBDMC40OTU0NzExMjcsMS44NzYwNSAwLjI0MDk4Mjc5OCwyLjAyMjk3ODg4IDAuMTAzMjc4MzQyLDIuMjYxNDg5OTkgQy0wLjAzNDQyNjExNCwyLjUwMDAwMTEgLTAuMDM0NDI2MTE0LDIuNzkzODU4OSAwLjEwMzI3ODM0MiwzLjAzMjM3MDAxIEMwLjI0MDk4Mjc5OCwzLjI3MDg4MTEyIDAuNDk1NDcxMTI3LDMuNDE3ODEgMC43NzA4ODAwMzIsMy40MTc4MSBMNC40MDI4NjAwMywzLjQxNzgxIEM0LjY3ODI2ODk0LDMuNDE3ODEgNC45MzI3NTcyNywzLjI3MDg4MTEyIDUuMDcwNDYxNzIsMy4wMzIzNzAwMSBDNS4yMDgxNjYxOCwyLjc5Mzg1ODkgNS4yMDgxNjYxOCwyLjUwMDAwMTEgNS4wNzA0NjE3MiwyLjI2MTQ4OTk5IEM0LjkzMjc1NzI3LDIuMDIyOTc4ODggNC42NzgyNjg5NCwxLjg3NjA1IDQuNDAyODYwMDMsMS44NzYwNSBMMC43NzA4ODAwMzIsMS44NzYwNSBaIiBpZD0i6Lev5b6EIj48L3BhdGg+CiAgICAgICAgICAgICAgICAgICAgPHBhdGggZD0iTTcuNzMwMDMwMDMsNy40OTcwNSBDNy40NTI0MDcwNCw3LjQ5MzczMzcyIDcuMTk0NDQxOSw3LjYzOTk0MzUgNy4wNTQ2NjQyNiw3Ljg3OTgzNDU5IEM2LjkxNDg4NjYxLDguMTE5NzI1NjcgNi45MTQ4ODY2MSw4LjQxNjI0NDMzIDcuMDU0NjY0MjYsOC42NTYxMzU0MSBDNy4xOTQ0NDE5LDguODk2MDI2NSA3LjQ1MjQwNzA0LDkuMDQyMjM2MjggNy43MzAwMzAwMyw5LjAzODkyIEwxMS4zNjIwMSw5LjAzODkyIEMxMS43ODc4MTYxLDkuMDM4OTIgMTIuMTMzLDguNjkzNzM2MDIgMTIuMTMzLDguMjY3OTMgQzEyLjEzMyw3Ljg0MjEyMzk4IDExLjc4NzgxNjEsNy40OTY5NCAxMS4zNjIwMSw3LjQ5Njk0IEw3LjczMDAzMDAzLDcuNDk3MDUgWiIgaWQ9Iui3r+W+hCI+PC9wYXRoPgogICAgICAgICAgICAgICAgICAgIDxwYXRoIGQ9Ik03LjczMDAzMDAzLDEwLjQwODg2IEM3LjQ1MjQwNzA0LDEwLjQwNTU0MzcgNy4xOTQ0NDE5LDEwLjU1MTc1MzUgNy4wNTQ2NjQyNiwxMC43OTE2NDQ2IEM2LjkxNDg4NjYxLDExLjAzMTUzNTcgNi45MTQ4ODY2MSwxMS4zMjgwNTQzIDcuMDU0NjY0MjYsMTEuNTY3OTQ1NCBDNy4xOTQ0NDE5LDExLjgwNzgzNjUgNy40NTI0MDcwNCwxMS45NTQwNDYzIDcuNzMwMDMwMDMsMTEuOTUwNzMgTDExLjM2MjAxLDExLjk1MDczIEMxMS42Mzc0NTgyLDExLjk1MDczIDExLjg5MTk4MjksMTEuODAzNzgwMiAxMi4wMjk3MDcsMTEuNTY1MjM1IEMxMi4xNjc0MzExLDExLjMyNjY4OTkgMTIuMTY3NDMxMSwxMS4wMzI3OTAxIDEyLjAyOTcwNywxMC43OTQyNDUgQzExLjg5MTk4MjksMTAuNTU1Njk5OCAxMS42Mzc0NTgyLDEwLjQwODc1IDExLjM2MjAxLDEwLjQwODc1IEw3LjczMDAzMDAzLDEwLjQwODg2IFoiIGlkPSLot6/lvoQiPjwvcGF0aD4KICAgICAgICAgICAgICAgICAgICA8cGF0aCBkPSJNNy41NTIyNzAwMywzLjQxNzM3IEw4LjY1ODQzMDAzLDMuNDE3MzcgTDguNjU4NDMwMDMsNC41MjA1NiBDOC42NTg0MzAwMyw0Ljk0NzY0MTggOS4wMDQ2NDgyNCw1LjI5Mzg2IDkuNDMxNzMwMDMsNS4yOTM4NiBDOS44NTg4MTE4Myw1LjI5Mzg2IDEwLjIwNTAzLDQuOTQ3NjQxOCAxMC4yMDUwMyw0LjUyMDU2IEwxMC4yMDUwMywzLjQxNzM3IEwxMS4zMTExOSwzLjQxNzM3IEMxMS43MzY5MzUzLDMuNDE3MzcgMTIuMDgyMDcsMy4wNzIyMzUyNyAxMi4wODIwNywyLjY0NjQ5IEMxMi4wODIwNywyLjIyMDc0NDczIDExLjczNjkzNTMsMS44NzU2MSAxMS4zMTExOSwxLjg3NTYxIEwxMC4yMDUwMywxLjg3NTYxIEwxMC4yMDUwMywwLjc3MzMgQzEwLjIwNTAzLDAuMzQ2MjE4MjAzIDkuODU4ODExODMsMCA5LjQzMTczMDAzLDAgQzkuMDA0NjQ4MjQsMCA4LjY1ODQzMDAzLDAuMzQ2MjE4MjAzIDguNjU4NDMwMDMsMC43NzMzIEw4LjY1ODQzMDAzLDEuODc2MDUgTDcuNTUyMjcwMDMsMS44NzYwNSBDNy4yNzI1NDk2NiwxLjg2OTM2NTc5IDcuMDExMTY4NzcsMi4wMTQ3NzMzMiA2Ljg2OTM0MzM2LDIuMjU1OTY1NjEgQzYuNzI3NTE3OTYsMi40OTcxNTc5IDYuNzI3NTE3OTYsMi43OTYyNjIxIDYuODY5MzQzMzYsMy4wMzc0NTQzOSBDNy4wMTExNjg3NywzLjI3ODY0NjY4IDcuMjcyNTQ5NjYsMy40MjQwNTQyMSA3LjU1MjI3MDAzLDMuNDE3MzcgWiIgaWQ9Iui3r+W+hCI+PC9wYXRoPgogICAgICAgICAgICAgICAgICAgIDxwYXRoIGQ9Ik0zLjQyNDMwMDAzLDcuNjk3NDcgTDIuNDcyOTEwMDMsOC42NDYzMyBMMS41MjExOTAwMyw3LjY5NzQ3IEMxLjIyMDE2NzIzLDcuMzk1MjkyOTIgMC43MzExNzcxMTEsNy4zOTQzNTcyIDAuNDI5MDAwMDMyLDcuNjk1MzggQzAuMTI2ODIyOTU0LDcuOTk2NDAyOCAwLjEyNTg4NzIzLDguNDg1MzkyOTIgMC40MjY5MTAwMzIsOC43ODc1NyBMMS4zNzg3NDAwMyw5LjczNjU0IEwwLjQyNjkxMDAzMiwxMC42ODU0IEMwLjEzMDIwNzA0MywxMC45ODgxMTgyIDAuMTMzMDMzMTk5LDExLjQ3MzQzMTUgMC40MzMyNDE2MjIsMTEuNzcyNjczNiBDMC43MzM0NTAwNDYsMTIuMDcxOTE1OCAxLjIxODc2OTk1LDEyLjA3MzE3NzQgMS41MjA1MzAwMywxMS43NzU1IEwyLjQ3MjI1MDAzLDEwLjgyNjY0IEwzLjQyMzk3MDAzLDExLjc3NTUgQzMuNjE4MTM2NzYsMTEuOTczNjAzMiAzLjkwMzY1MTE0LDEyLjA1MjMyMjQgNC4xNzE5MDQ2MiwxMS45ODE3MTMzIEM0LjQ0MDE1ODExLDExLjkxMTEwNDIgNC42NDk5MTc0NSwxMS43MDIwMiA0LjcyMTM5MTAyLDExLjQzMzk5NTUgQzQuNzkyODY0NiwxMS4xNjU5NzEgNC43MTUwNjYyLDEwLjg4MDIwNDQgNC41MTc1OTAwMywxMC42ODU0IEwzLjU2NTc2MDAzLDkuNzM2NTQgTDQuNTE3NTkwMDMsOC43ODc1NyBDNC42NjI2MDcyNCw4LjY0MzI3OTQ2IDQuNzQ0MTY1ODQsOC40NDcxNjA2NyA0Ljc0NDIyNzc3LDguMjQyNTg4NTYgQzQuNzQ0Mjg5NjksOC4wMzgwMTY0NSA0LjY2Mjg0OTg2LDcuODQxODQ4MzEgNC41MTc5MjAwMyw3LjY5NzQ3IEM0LjIxNTUyNjE3LDcuMzk2NDQwMDggMy43MjY2OTM5LDcuMzk2NDQwMDggMy40MjQzMDAwMyw3LjY5NzQ3IFoiIGlkPSLot6/lvoQiPjwvcGF0aD4KICAgICAgICAgICAgICAgIDwvZz4KICAgICAgICAgICAgPC9nPgogICAgICAgIDwvZz4KICAgIDwvZz4KPC9zdmc+" alt="">
													</a>
												</div>
                                                <div class="actions">
                                                    <a href="<?php echo home_url('so-sanh-xe'); ?>" class="compare-btn"><?php esc_html_e('+ So sánh  ', 'your-textdomain'); ?></a>
                                                </div>
                                            </div>
                                        </div>
                                    <?php } ?>
                                <?php } ?>
                            </div>
                        </div>
                    <?php } ?>


                    <?php if (!empty($variants_not_on_sale)) { ?>

                        <div id="tab-not-on-sale" class=" <?php echo (!empty($variants_not_on_sale) && empty($variants_on_sale)) ? 'active' : 'custom-tab-panel' ?>">
                            <div class="car-list-varient">

                                <?php $grouped_variants = []; ?>
                                <?php foreach ($variants_not_on_sale as $variant) {
                                    $variant_id = $variant->ID;
                                    $meta_data = $variants_post_meta[$variant_id];

                                    $launched_year = isset($meta_data['launched_year'][0]) ? $meta_data['launched_year'][0] : 'N/A';
                                    $engine = isset($meta_data['engine'][0]) ? $meta_data['engine'][0] : 'N/A';
                                    $aspiration_form = isset($meta_data['aspiration_form'][0]) ? $meta_data['aspiration_form'][0] : 'N/A';
                                    $launched_year = isset($meta_data['launched_year'][0]) ? $meta_data['launched_year'][0] : 'N/A';
                                    $engine = isset($meta_data['engine'][0]) ? $meta_data['engine'][0] : 'N/A';
                                    $aspiration_form = isset($meta_data['aspiration_form'][0]) ? $meta_data['aspiration_form'][0] : 'N/A';
                                    // Group key based on combined values
                                    $group_key = $launched_year . ' | ' . $engine . ' | ' . $aspiration_form;

                                    // Add post to the grouped array
                                    $grouped_variants[$group_key][] = [
                                        'title' => $variant->post_title,
                                        'retail_price' => (isset($meta_data['retail_price'][0]) && $meta_data['retail_price'][0] != 0) ? format_price_vietnam($meta_data['retail_price'][0]) : 'Đang cập nhật',
                                        'monthly_payment' => (isset($meta_data['monthly_payment'][0]) && $meta_data['monthly_payment'][0] != 0) ? format_price_vietnam($meta_data['monthly_payment'][0]) : 'Đang cập nhật',
                                    ];
                                }
                                wp_reset_postdata();

                                // Display grouped data
                                foreach ($grouped_variants as $group_key => $variants) {
                                ?>
                                    <div class="header">
                                        <span><?php echo esc_html($group_key); ?></span>
                                        <span>Giá xe</span>
                                    </div>
                                    <?php foreach ($variants as $variant) { ?>
                                        <div class="car-item-c">
											<?php
												$base_url = get_home_url();
												$make = get_query_var('make');
												$model = get_query_var('model');
												$variant_url = $base_url . '/xe-oto/' . $make . '/' . $model . '/' . $variant['post_name'];
                                            ?>
                                            <div class="car-name"><a href="<?php echo $variant_url; ?>"><?php echo esc_html($variant['title']); ?></a></div>
                                            <div class="right-content">
                                                <div class="price-section">
                                                    <div class="total-price"><?php echo $variant['retail_price']; ?></div>
                                                    <div class="monthly-price"><?php echo $variant['monthly_payment'] . '/tháng'; ?></div>
                                                </div>
                                                <div class="calculator-icon">
													<a href="<?php echo home_url('dung-cu/mua-xe-tra-gop'); ?>">
                                                    <img src="data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPHN2ZyB3aWR0aD0iMjJweCIgaGVpZ2h0PSIyMnB4IiB2aWV3Qm94PSIwIDAgMjIgMjIiIHZlcnNpb249IjEuMSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIiB4bWxuczp4bGluaz0iaHR0cDovL3d3dy53My5vcmcvMTk5OS94bGluayI+CiAgICA8dGl0bGU+6K6h566X5ZmoPC90aXRsZT4KICAgIDxnIGlkPSLmjqfku7YiIHN0cm9rZT0ibm9uZSIgc3Ryb2tlLXdpZHRoPSIxIiBmaWxsPSJub25lIiBmaWxsLXJ1bGU9ImV2ZW5vZGQiPgogICAgICAgIDxnIGlkPSJWYXJpYW50c19saXN0IiB0cmFuc2Zvcm09InRyYW5zbGF0ZSgtNTczLjAwMDAwMCwgLTI2LjAwMDAwMCkiIGZpbGwtcnVsZT0ibm9uemVybyI+CiAgICAgICAgICAgIDxnIGlkPSLorqHnrpflmagiIHRyYW5zZm9ybT0idHJhbnNsYXRlKDU3NC4wMDAwMDAsIDI3LjAwMDAwMCkiPgogICAgICAgICAgICAgICAgPHJlY3QgaWQ9IuefqeW9oiIgc3Ryb2tlPSIjQkZCRkJGIiBmaWxsLW9wYWNpdHk9IjAiIGZpbGw9IiNGRkZGRkYiIHg9IjAiIHk9IjAiIHdpZHRoPSIyMCIgaGVpZ2h0PSIyMCIgcng9IjIiPjwvcmVjdD4KICAgICAgICAgICAgICAgIDxnIGlkPSLnvJbnu4QiIGNsYXNzPSJjYWxjIiB0cmFuc2Zvcm09InRyYW5zbGF0ZSg0LjAwMDAwMCwgNC4wMDAwMDApIiBmaWxsPSIjMjYyNjI2Ij4KICAgICAgICAgICAgICAgICAgICA8cGF0aCBkPSJNMC43NzA4ODAwMzIsMS44NzYwNSBDMC40OTU0NzExMjcsMS44NzYwNSAwLjI0MDk4Mjc5OCwyLjAyMjk3ODg4IDAuMTAzMjc4MzQyLDIuMjYxNDg5OTkgQy0wLjAzNDQyNjExNCwyLjUwMDAwMTEgLTAuMDM0NDI2MTE0LDIuNzkzODU4OSAwLjEwMzI3ODM0MiwzLjAzMjM3MDAxIEMwLjI0MDk4Mjc5OCwzLjI3MDg4MTEyIDAuNDk1NDcxMTI3LDMuNDE3ODEgMC43NzA4ODAwMzIsMy40MTc4MSBMNC40MDI4NjAwMywzLjQxNzgxIEM0LjY3ODI2ODk0LDMuNDE3ODEgNC45MzI3NTcyNywzLjI3MDg4MTEyIDUuMDcwNDYxNzIsMy4wMzIzNzAwMSBDNS4yMDgxNjYxOCwyLjc5Mzg1ODkgNS4yMDgxNjYxOCwyLjUwMDAwMTEgNS4wNzA0NjE3MiwyLjI2MTQ4OTk5IEM0LjkzMjc1NzI3LDIuMDIyOTc4ODggNC42NzgyNjg5NCwxLjg3NjA1IDQuNDAyODYwMDMsMS44NzYwNSBMMC43NzA4ODAwMzIsMS44NzYwNSBaIiBpZD0i6Lev5b6EIj48L3BhdGg+CiAgICAgICAgICAgICAgICAgICAgPHBhdGggZD0iTTcuNzMwMDMwMDMsNy40OTcwNSBDNy40NTI0MDcwNCw3LjQ5MzczMzcyIDcuMTk0NDQxOSw3LjYzOTk0MzUgNy4wNTQ2NjQyNiw3Ljg3OTgzNDU5IEM2LjkxNDg4NjYxLDguMTE5NzI1NjcgNi45MTQ4ODY2MSw4LjQxNjI0NDMzIDcuMDU0NjY0MjYsOC42NTYxMzU0MSBDNy4xOTQ0NDE5LDguODk2MDI2NSA3LjQ1MjQwNzA0LDkuMDQyMjM2MjggNy43MzAwMzAwMyw5LjAzODkyIEwxMS4zNjIwMSw5LjAzODkyIEMxMS43ODc4MTYxLDkuMDM4OTIgMTIuMTMzLDguNjkzNzM2MDIgMTIuMTMzLDguMjY3OTMgQzEyLjEzMyw3Ljg0MjEyMzk4IDExLjc4NzgxNjEsNy40OTY5NCAxMS4zNjIwMSw3LjQ5Njk0IEw3LjczMDAzMDAzLDcuNDk3MDUgWiIgaWQ9Iui3r+W+hCI+PC9wYXRoPgogICAgICAgICAgICAgICAgICAgIDxwYXRoIGQ9Ik03LjczMDAzMDAzLDEwLjQwODg2IEM3LjQ1MjQwNzA0LDEwLjQwNTU0MzcgNy4xOTQ0NDE5LDEwLjU1MTc1MzUgNy4wNTQ2NjQyNiwxMC43OTE2NDQ2IEM2LjkxNDg4NjYxLDExLjAzMTUzNTcgNi45MTQ4ODY2MSwxMS4zMjgwNTQzIDcuMDU0NjY0MjYsMTEuNTY3OTQ1NCBDNy4xOTQ0NDE5LDExLjgwNzgzNjUgNy40NTI0MDcwNCwxMS45NTQwNDYzIDcuNzMwMDMwMDMsMTEuOTUwNzMgTDExLjM2MjAxLDExLjk1MDczIEMxMS42Mzc0NTgyLDExLjk1MDczIDExLjg5MTk4MjksMTEuODAzNzgwMiAxMi4wMjk3MDcsMTEuNTY1MjM1IEMxMi4xNjc0MzExLDExLjMyNjY4OTkgMTIuMTY3NDMxMSwxMS4wMzI3OTAxIDEyLjAyOTcwNywxMC43OTQyNDUgQzExLjg5MTk4MjksMTAuNTU1Njk5OCAxMS42Mzc0NTgyLDEwLjQwODc1IDExLjM2MjAxLDEwLjQwODc1IEw3LjczMDAzMDAzLDEwLjQwODg2IFoiIGlkPSLot6/lvoQiPjwvcGF0aD4KICAgICAgICAgICAgICAgICAgICA8cGF0aCBkPSJNNy41NTIyNzAwMywzLjQxNzM3IEw4LjY1ODQzMDAzLDMuNDE3MzcgTDguNjU4NDMwMDMsNC41MjA1NiBDOC42NTg0MzAwMyw0Ljk0NzY0MTggOS4wMDQ2NDgyNCw1LjI5Mzg2IDkuNDMxNzMwMDMsNS4yOTM4NiBDOS44NTg4MTE4Myw1LjI5Mzg2IDEwLjIwNTAzLDQuOTQ3NjQxOCAxMC4yMDUwMyw0LjUyMDU2IEwxMC4yMDUwMywzLjQxNzM3IEwxMS4zMTExOSwzLjQxNzM3IEMxMS43MzY5MzUzLDMuNDE3MzcgMTIuMDgyMDcsMy4wNzIyMzUyNyAxMi4wODIwNywyLjY0NjQ5IEMxMi4wODIwNywyLjIyMDc0NDczIDExLjczNjkzNTMsMS44NzU2MSAxMS4zMTExOSwxLjg3NTYxIEwxMC4yMDUwMywxLjg3NTYxIEwxMC4yMDUwMywwLjc3MzMgQzEwLjIwNTAzLDAuMzQ2MjE4MjAzIDkuODU4ODExODMsMCA5LjQzMTczMDAzLDAgQzkuMDA0NjQ4MjQsMCA4LjY1ODQzMDAzLDAuMzQ2MjE4MjAzIDguNjU4NDMwMDMsMC43NzMzIEw4LjY1ODQzMDAzLDEuODc2MDUgTDcuNTUyMjcwMDMsMS44NzYwNSBDNy4yNzI1NDk2NiwxLjg2OTM2NTc5IDcuMDExMTY4NzcsMi4wMTQ3NzMzMiA2Ljg2OTM0MzM2LDIuMjU1OTY1NjEgQzYuNzI3NTE3OTYsMi40OTcxNTc5IDYuNzI3NTE3OTYsMi43OTYyNjIxIDYuODY5MzQzMzYsMy4wMzc0NTQzOSBDNy4wMTExNjg3NywzLjI3ODY0NjY4IDcuMjcyNTQ5NjYsMy40MjQwNTQyMSA3LjU1MjI3MDAzLDMuNDE3MzcgWiIgaWQ9Iui3r+W+hCI+PC9wYXRoPgogICAgICAgICAgICAgICAgICAgIDxwYXRoIGQ9Ik0zLjQyNDMwMDAzLDcuNjk3NDcgTDIuNDcyOTEwMDMsOC42NDYzMyBMMS41MjExOTAwMyw3LjY5NzQ3IEMxLjIyMDE2NzIzLDcuMzk1MjkyOTIgMC43MzExNzcxMTEsNy4zOTQzNTcyIDAuNDI5MDAwMDMyLDcuNjk1MzggQzAuMTI2ODIyOTU0LDcuOTk2NDAyOCAwLjEyNTg4NzIzLDguNDg1MzkyOTIgMC40MjY5MTAwMzIsOC43ODc1NyBMMS4zNzg3NDAwMyw5LjczNjU0IEwwLjQyNjkxMDAzMiwxMC42ODU0IEMwLjEzMDIwNzA0MywxMC45ODgxMTgyIDAuMTMzMDMzMTk5LDExLjQ3MzQzMTUgMC40MzMyNDE2MjIsMTEuNzcyNjczNiBDMC43MzM0NTAwNDYsMTIuMDcxOTE1OCAxLjIxODc2OTk1LDEyLjA3MzE3NzQgMS41MjA1MzAwMywxMS43NzU1IEwyLjQ3MjI1MDAzLDEwLjgyNjY0IEwzLjQyMzk3MDAzLDExLjc3NTUgQzMuNjE4MTM2NzYsMTEuOTczNjAzMiAzLjkwMzY1MTE0LDEyLjA1MjMyMjQgNC4xNzE5MDQ2MiwxMS45ODE3MTMzIEM0LjQ0MDE1ODExLDExLjkxMTEwNDIgNC42NDk5MTc0NSwxMS43MDIwMiA0LjcyMTM5MTAyLDExLjQzMzk5NTUgQzQuNzkyODY0NiwxMS4xNjU5NzEgNC43MTUwNjYyLDEwLjg4MDIwNDQgNC41MTc1OTAwMywxMC42ODU0IEwzLjU2NTc2MDAzLDkuNzM2NTQgTDQuNTE3NTkwMDMsOC43ODc1NyBDNC42NjI2MDcyNCw4LjY0MzI3OTQ2IDQuNzQ0MTY1ODQsOC40NDcxNjA2NyA0Ljc0NDIyNzc3LDguMjQyNTg4NTYgQzQuNzQ0Mjg5NjksOC4wMzgwMTY0NSA0LjY2Mjg0OTg2LDcuODQxODQ4MzEgNC41MTc5MjAwMyw3LjY5NzQ3IEM0LjIxNTUyNjE3LDcuMzk2NDQwMDggMy43MjY2OTM5LDcuMzk2NDQwMDggMy40MjQzMDAwMyw3LjY5NzQ3IFoiIGlkPSLot6/lvoQiPjwvcGF0aD4KICAgICAgICAgICAgICAgIDwvZz4KICAgICAgICAgICAgPC9nPgogICAgICAgIDwvZz4KICAgIDwvZz4KPC9zdmc+" alt="">
                                                </a>
												</div>
                                                <div class="actions">
                                                    <a href="<?php echo home_url('so-sanh-xe'); ?>" class="compare-btn"><?php esc_html_e('+ So sánh  ', 'your-textdomain'); ?></a>
                                                </div>
                                            </div>
                                        </div>
                                    <?php } ?>
                                <?php } ?>
                            </div>
                        </div>
                    <?php } ?>

                </div>
            </div>
        </div>
    </div>

    <script>
        document.addEventListener("DOMContentLoaded", function() {
            // Hide Not On Sale tab content on load
            const onSaleTab = document.querySelector('[data-tab="tab-on-sale"]');
            const notOnSaleTab = document.querySelector('[data-tab="tab-not-on-sale"]');
            const onSaleContent = document.getElementById("tab-on-sale");
            const notOnSaleContent = document.getElementById("tab-not-on-sale");

            // Show only the On Sale tab content initially
            onSaleContent.style.display = "block";
            notOnSaleContent.style.display = "none";

            // Add event listener to show/hide content on tab click
            onSaleTab.addEventListener("click", function() {
                onSaleContent.style.display = "block";
                notOnSaleContent.style.display = "none";
                onSaleTab.classList.add("custom-tab-active");
                notOnSaleTab.classList.remove("custom-tab-active");
            });

            notOnSaleTab.addEventListener("click", function() {
                onSaleContent.style.display = "none";
                notOnSaleContent.style.display = "block";
                onSaleTab.classList.remove("custom-tab-active");
                notOnSaleTab.classList.add("custom-tab-active");
            });
        });
    </script>
<?php
}
add_shortcode('single_listing_variants', 'single_listing_variants')
?>