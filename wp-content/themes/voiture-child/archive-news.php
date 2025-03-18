<?php
get_header();
$sidebar_configs = voiture_get_blog_layout_configs();
$current_url = trim(parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH), '/');

// $second_lang = get_category_status() ? '' : get_current_language();
?>

<div class='above-breadcrumb-ad'>
    <!-- /22557728108/my_news_breadcrumb_above_pc -->
    <?php echo do_shortcode('[dynamic_ad_unit ad_id="VN_News_Breadcrumb_Above_PC"]'); ?>
</div>

<div class="news-breadcrumb container">
    <?php
    echo do_shortcode('[breadcrumb]');
    ?>
</div>

<div class="container" style="display: flex; align-items: center; padding-bottom: 15px;">

    <span>
        <h1 class="wa-title-text archive-news-latest-news">AutoFun Tin tức</h1>
    </span>
    <span>
        <?php
//         echo do_shortcode('[custom_language_switcher page="news"]');
        ?>
    </span>
</div>
<section class="category-subcategory">
    <div id="news-categories-tabs" class="news-categories-tabs">
        <div class="navbarcls container ">
            <!-- Scroll Left Button -->
            <div class="button-container-left">
                <button id="scroll-left" class="scroll-btn-left" aria-label="Scroll left" style="margin-right: 10px">
                    <i class="fas fa-chevron-left"></i>
                </button>
            </div>
            <!-- Category Tabs -->
            <?php
            $current_language = isset($_COOKIE['preferred_language']) ? $_COOKIE['preferred_language'] : 'English';

            // Determine the base URL and label based on language
            if ($current_language == 'Bahasa Malaysia') {
                $base_url = home_url() . '/bm';
                $label = 'Terkini';
            } elseif ($current_language == '中文') {
                $base_url = home_url() . '/zh';
                $label = '最新';
            } else {
                $base_url = home_url() . '/tin-tuc/moi-nhat';
                $label = 'Mới nhất';
            }
			
			$url_path = $_SERVER['REQUEST_URI'];
            if (strpos($url_path, '/tin-tuc') !== false) {
                $base_url = home_url() . '/tin-tuc/moi-nhat';
                $label = 'Mới nhất';
            }

            ?>
            <ul id="category-tabs" class="category-tabs  ">
                <li
                    class="tab-link <?php echo ($current_url == 'tin-tuc/moi-nhat' || $current_url == 'bm' || $current_url == 'zh') ? 'selected' : ''; ?> <?php echo ($current_url == 'tin-tuc') ? 'selected' : ''; ?>"
                    data-category="0"
                    onclick="window.location.href='<?php echo $base_url; ?>';">
                    <?php echo $label; ?>
                </li>
                <?php
                $categories = get_terms('news-category', array(
                    'hide_empty' => false,
                    'parent' => 0,
                    'meta_query' => array(
//                         array(
//                             'key'     => 'second_lang',
//                             'value'   => $second_lang, // Empty value
//                             'compare' => 'IN', // Either empty or not set
//                         ),
                        array(
                            'key'     => 'state',
                            'value'   => '1', // Value of the 'state' field should be 1
                            'compare' => '=' // Exact match for state = 1
                        ),
                        array(
                            'key'     => 'type',
                            'value'   => '1', // Value of the 'type' field should be 1
                            'compare' => '=' // Exact match for type = 1
                        ),
                    ),
                    'orderby'    => 'meta_value', // Sort by term meta value
                    'meta_key'   => 'sort',
                    'order'      => 'ASC',
                ));
                foreach ($categories as $category) :
                    $category_slug = sanitize_title($category->name);
                    $category_url = 'tin-tuc/' . $category_slug;
                    $is_selected = ($current_url == $category_url) ? 'selected' : '';
                ?>
                    <li class="tab-link <?php echo $is_selected; ?>"
                        data-category="<?php echo esc_attr($category->term_id); ?>"
                        data-categoryName="<?php echo esc_attr($category->name); ?>"
                        data-categorySlug="<?php echo esc_attr($category->slug); ?>">
                        <?php echo esc_html($category->name); ?>
                    </li>
                <?php endforeach; ?>
            </ul>


            <!-- Scroll Right Button -->
            <div class="button-container-right">
                <button id="scroll-right" class="scroll-btn-right" aria-label="Scroll right" style="margin-left: 20px">
                    <i class="fas fa-chevron-right"></i>
                </button>
            </div>
        </div>
    </div>
    <div id="subcategories-container" class="subcategories-container container">
    </div>
    <div id="loader" class="loader">
        <i class="spinner"></i>
    </div>
    <input type="hidden" name="parent-category" id="selected-category-id" value="" />
</section>
<section id="main-container" class="main-content <?php echo apply_filters('voiture_blog_content_class', 'container'); ?> inner">
    <?php voiture_before_content($sidebar_configs); ?>
    <div class="row responsive-medium archive-news-col">
        <?php voiture_display_sidebar_left($sidebar_configs); ?>
        <div id="main-content" class="main-blog col-sm-12 <?php echo esc_attr($sidebar_configs['main']['class']); ?>">
            <div id="main" class="site-main layout-blog" role="main">
                <div id="news-articles">
                    <?php if (have_posts()) : ?>
                        <header class="page-header hidden">
                            <?php
                            the_archive_title('<h1 class="page-title">', '</h1>');
                            the_archive_description('<div class="taxonomy-description">', '</div>');
                            ?>
                        </header>
                    <?php
                    // $layout = voiture_get_config( 'blog_display_mode', 'list' );
                    // get_template_part( 'template-posts/layouts/'.$layout);
                    // voiture_paging_nav();
                    else :
                        get_template_part('template-posts/content', 'none');
                    endif;
                    ?>
                </div>
                <div class="view-more" style="display: flex; justify-content:center; text-align: center;">
                    <a href="#" id="load-more-link" style="display:none; text-decoration:none; color:#576b95; display:block; text-align:center; margin:20px 0;font-weight:600;">
                        Xem thêm <i class="fas fa-chevron-down" style="margin-left:5px;"></i>
                    </a>
                    <div id="loader" style="display:none; margin:20px 0;">
                        <i class="fas fa-spinner fa-spin" style="font-size:20px; color:#576b95 ;"></i>
                    </div>
                </div>
            </div><!-- .site-main -->
        </div><!-- .content-area -->
        <!-- <h3> Popular Models</h3> -->
        <div class="col-sm-12 col-md-4 col-lg-3 col-xs-12 recommended-cars-wrapper">
            <?php echo do_shortcode('[recommended_cars]'); ?>
            <?php 
// 			echo do_shortcode('[popular_car_videos]'); 
			?>
			<?php echo do_shortcode('[dynamic_ad_unit ad_id="VN_News_Sidebar_End_PC"]'); ?>
        </div>
    </div>

    <!-- ad unit -->
    <script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=ca-pub-8521211126902149" crossorigin="anonymous"></script>
    <ins class="adsbygoogle"
        style="display:block"
        data-ad-client="ca-pub-8521211126902149"
        data-ad-slot="7561364664"
        data-ad-format="auto"
        data-full-width-responsive="true"></ins>
    <script>
        (adsbygoogle = window.adsbygoogle || []).push({});
    </script>

    <?php echo do_shortcode('[elementor-template id="4891"]'); ?>
    <?php echo do_shortcode('[elementor-template id="31611"]'); ?>
</section>
<style>
    .recommended-cars-wrapper {
        max-width: 300px;
        /* Adjust this width as per your design */
        margin: 0 auto;
        /* Center the content */
        width: 100%;
        /* Ensures it takes full width of its parent up to the max-width */
    }
	.archive-news-col{
		padding-top:50px !important;
	}
    .recommended-cars-wrapper.full-width {
        max-width: none;
        /* Allows the section to expand if needed */
        width: 100%;
        /* Forces the element to take up full width if explicitly needed */
    }
	@media screen and (max-width: 768px) {
		.recommended-cars-wrapper {
			max-width: 100% !important;
			margin: 0 auto;
			width: 100%;
		}
	}
</style>
<?php
// }
get_footer(); ?>