<?php
function dynamic_navigation_shortcode()
{
$translate = [
    
'You May Also Like' => 'Cùng Mục',
'View Model' => 'Xem dòng xe',
'News Navigation' => 'Điều hướng Tin tức',
'News' => 'Tin tức'

];
    // Array for "You May Also Like"
    $make = get_query_var('make');
    $model = get_query_var('model');
    $listing_name = $make . '-' . $model;

    // get listing post by post name
    $listing_post = get_posts(array(
        'name' => $listing_name,
        'post_type' => 'listing',
        'posts_per_page' => 1
    ));
    $post_id = $listing_post[0]->ID;
    $post_title = $listing_post[0]->post_title;
    $make_id = get_post_meta($post_id, '_listing_make', true);

    $may_also_like = [
        ['title' => ' Hình ảnh ' . $post_title ,'link' => '/xe-oto/' . $make . '/' . $model . '/hinh-anh'],
        ['title' => ' Thông số kỹ thuật '  . $post_title  , 'link' => '/xe-oto/' . $make . '/' . $model . '/thong-so-ky-thuat'],
        ['title' => ' Tiêu thụ nhiên liệu ' . $post_title  , 'link' => '/xe-oto/' . $make . '/' . $model . '/tieu-hao-nhien-lieu'],
        ['title' => ' Màu sắc ' . $post_title  , 'link' => '/xe-oto/' . $make . '/' . $model . '/mau-sac'],
    ];

    // Array for "News Navigation"
    $related_brand_models = get_posts(array(
        'post_type' => 'listing',
        'posts_per_page' => 10,
        'meta_query' => array(
            array(
                'key' => '_listing_make',
                'value' => $make_id
            )
        )
    ));
    $news_navigation = [];
    $price_list = [];
    foreach ($related_brand_models as $related_brand_model) {
        $post_name = $related_brand_model->post_name;
        $name_array = explode("-", $post_name);
        // remove first element and append other with hyphen
        $post_name = implode("-", array_slice($name_array, 1));
        $news_navigation[] = ['title' => $related_brand_model->post_title . ' tin-tuc', 'link' => '/xe-oto/' . $make . '/' . $post_name . '/tin-tuc'];

        $price_list[] = ['title' => $related_brand_model->post_title . ' Giá', 'link' => '/xe-oto/' . $make . '/' . $post_name];
    }

    // HTML Output
    ob_start(); ?>

    <div class="dynamic-navigation">
        <h2 class="wa-title-text nav-head"><?php echo $translate['You May Also Like']; ?></h2>
        <ul class="also-like">
            <?php foreach ($may_also_like as $item): ?>
                <li><a href="<?php echo esc_url($item['link']); ?>">
                        <?php echo esc_html($item['title']); ?>
                        <i class="fa fa-chevron-right"></i>
                    </a></li>
            <?php endforeach; ?>
        </ul>

        <h2 class="wa-title-text nav-head"> <?php echo $translate['News Navigation']; ?></h2>
        <ul class="news-navigation">
            <?php foreach ($news_navigation as $item): ?>
                <li><a href="<?php echo esc_url($item['link']); ?>">
                        <?php echo esc_html($item['title']); ?>
                        <i class="fa fa-chevron-right"></i>
                    </a></li>
            <?php endforeach; ?>
        </ul>

        <h2 class="wa-title-text nav-head"> Bảng Giá Xe ô tô <?php echo ucfirst($make); ?> </h2>
        <ul class="price-list">
            <?php foreach ($price_list as $item): ?>
                <li><a href="<?php echo esc_url($item['link']); ?>">
                        <?php echo esc_html($item['title']); ?>
                        <i class="fa fa-chevron-right"></i>
                    </a></li>
            <?php endforeach; ?>
        </ul>
    </div>

    <style>
        /* General Styling */

        .nav-head {
            margin-top: 63px;

        }

        .dynamic-navigation h4 {
            font-weight: bold;
            margin-bottom: 10px;
            margin-top: 55px;

        }

        /* Styling for both lists */
        .dynamic-navigation ul {
            list-style: none;
            padding: 0;
            margin: 0 0 -37px 0;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            padding-left: 10px;
            padding-right: 10px;
            width: 275px;
        }

        /* List item styling */
        .dynamic-navigation li {
            margin: 0;
            border-bottom: 1px solid #e0e0e0;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        /* Remove border for last item */
        .dynamic-navigation li:last-child {
            border-bottom: none;
        }

        /* Link styling */
        .dynamic-navigation a {
            font-size: 14px;
            font-family: 'Roboto';
            text-decoration: none;
            color: #262626;
            padding: 10px 0;
            display: flex;
            justify-content: space-between;
            width: 100%;
        }

        /* Font Awesome Icon Styling */
        .dynamic-navigation a i {
            margin-left: 10px;
            color: #262626;
            transition: color 0.3s ease;
            margin-top: 7px;
        }

        /* Hover Effect for Links */
        .dynamic-navigation a:hover {
            color: #ffb400;
        }

        .dynamic-navigation a:hover i {
            color: #ffb400;
        }
    </style>

    <!-- Include Font Awesome -->
    <script src="https://kit.fontawesome.com/a076d05399.js" crossorigin="anonymous"></script>

<?php
    return ob_get_clean();
}
add_shortcode('dynamic_navigation', 'dynamic_navigation_shortcode');
